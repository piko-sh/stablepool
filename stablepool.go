// Copyright 2026 PolitePixels Limited
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This project stands against fascism, authoritarianism, and all forms of
// oppression. We built this to empower people, not to enable those who would
// strip others of their rights and dignity.

package stablepool

import (
	"errors"
	"fmt"
	"math/bits"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"

	"piko.sh/safeconv"
)

const (
	// gcAwareDrainAttempts caps fresh allocations the GC-aware Drain() tolerates before
	// stopping. Without it Drain() would allocate without end.
	gcAwareDrainAttempts = 4
)

const (
	// modePersistent holds objects in a pool-owned slab that survives every GC cycle.
	// Objects are only released when the Pool itself becomes unreachable.
	modePersistent mode = iota

	// ModeGCAware mirrors sync.Pool reclamation so pooled objects may be dropped under
	// memory pressure.
	ModeGCAware
)

var (
	// ErrNoInitialiser indicates New() was called without an initialise function.
	ErrNoInitialiser = errors.New("stablepool: initialise function required")

	// ErrBadCapacity indicates New() was called with a non-positive capacity.
	ErrBadCapacity = errors.New("stablepool: capacity must be > 0")

	// ErrBadGrowth indicates WithGrowth() was configured with a maximum below the initial
	// capacity.
	ErrBadGrowth = errors.New("stablepool: growth maximum must be >= initial capacity")

	// ErrBadShardCount indicates WithShardCount() was given a non-positive value.
	ErrBadShardCount = errors.New("stablepool: shard count must be > 0")

	// ErrBadLink indicates the pooled type does not embed Link as its first field.
	ErrBadLink = errors.New("stablepool: T must embed Link as its first field")
)

// mode selects the pool's storage and reclamation strategy.
type mode int

// Option customises pool construction.
type Option[T any] func(*config[T])

// Pool is a typed, pre-warmed object pool. Storage behaviour is selected at construction
// time by mode (see WithMode()).
type Pool[T any] struct {
	// initialise runs once per slot at construction and on growth to set up a fresh T.
	initialise func(*T)

	// cleaner runs on every Put() to reset user-visible state before reuse.
	cleaner func(*T)

	// gcInner backs ModeGCAware operation with a standard sync.Pool.
	gcInner *sync.Pool

	// shards holds the per-shard fast slot and free stack for modePersistent.
	shards []shard[T]

	// slabs records every allocated slab so the pre-warmed memory stays reachable.
	slabs [][]*T

	// mode records the storage strategy chosen at construction.
	mode mode

	// capacity holds the current total slab capacity in objects.
	capacity int

	// maxCapacity holds the growth ceiling. Zero disables growth.
	maxCapacity int

	// inFlight tracks objects handed out but not yet returned when metrics are enabled.
	inFlight atomic.Int64

	// gcFresh counts the objects gcInner.New allocated, so Drain() can tell a cached object
	// from a fresh one (sync.Pool.Get() never reports nil once New is set).
	gcFresh atomic.Int64

	// slabsMu guards slabs and capacity during growth.
	slabsMu sync.Mutex

	// shardMask equals len(shards) - 1 and replaces modulo with a one-cycle AND.
	shardMask uint32

	// metrics toggles the in-flight counter on Get() and Put().
	metrics bool
}

// New creates and pre-warms a Pool[T] with the given capacity.
//
// Takes initialise (func(*T)) which is the per-slot constructor; must be non-nil.
// Takes clean (func(*T)) which is an optional reset function run on every Put; may be
// nil.
// Takes capacity (int) which is the initial slab capacity; must be positive.
// Takes options (...Option[T]) which is zero or more Option values to customise
// construction.
//
// Returns *Pool[T] which is the constructed pool when configuration succeeds.
// Returns error which is ErrNoInitialiser, ErrBadCapacity, ErrBadShardCount,
// ErrBadGrowth, or ErrBadLink when the configuration is rejected.
func New[T any](initialise func(*T), clean func(*T), capacity int, options ...Option[T]) (*Pool[T], error) {
	poolConfig, err := resolvePoolConfig(initialise, capacity, options)
	if err != nil {
		return nil, err
	}
	if poolConfig.mode == modePersistent {
		if err := validateLinkPlacement[T](); err != nil {
			return nil, err
		}
	}
	p := &Pool[T]{mode: poolConfig.mode,
		initialise:  initialise,
		cleaner:     clean,
		maxCapacity: poolConfig.maxCapacity,
		metrics:     poolConfig.metrics, gcInner: nil, gcFresh: atomic.Int64{}, shards: nil, slabs: nil, capacity: 0, inFlight: atomic.Int64{}, slabsMu: sync.Mutex{}, shardMask: 0}
	if err := p.bootstrapStorage(poolConfig, initialise, capacity); err != nil {
		return nil, err
	}
	return p, nil
}

// Get returns an object from the pool, or nil when the pool is drained and growth is
// exhausted. In ModeGCAware, Get always succeeds via the initialise function.
//
// Returns *T which is a pooled object, or nil when the persistent pool is drained and
// growth is exhausted.
func (p *Pool[T]) Get() *T {
	if p.mode == ModeGCAware {
		object := p.popGCInner()
		if p.metrics && object != nil {
			p.inFlight.Add(1)
		}
		return object
	}

	object := p.getPersistent()
	if p.metrics && object != nil {
		p.inFlight.Add(1)
	}
	return object
}

// MustGet returns an object from the pool, allocating a fresh one when the pool is
// drained and the fallback object joins the free stack on Put.
//
// Returns *T which is a pooled or freshly allocated object.
func (p *Pool[T]) MustGet() *T {
	if object := p.Get(); object != nil {
		return object
	}
	object := new(T)
	p.initialise(object)
	if p.metrics {
		p.inFlight.Add(1)
	}
	return object
}

// GetPreferParked returns an object parked in a per-P slot when any slot holds one, the
// calling P's slot first and then every other P's, before falling back to MustGet's free
// stacks and growth.
//
// Returns *T which is a parked, pooled or freshly allocated object.
func (p *Pool[T]) GetPreferParked() *T {
	if p.mode == ModeGCAware {
		return p.MustGet()
	}
	object := p.takeParked()
	if object == nil {
		return p.MustGet()
	}
	if p.metrics {
		p.inFlight.Add(1)
	}
	return object
}

// Put returns an object to the pool.
//
// Takes object (*T) which is the object to return. A nil value is ignored.
func (p *Pool[T]) Put(object *T) {
	if object == nil {
		return
	}
	if p.cleaner != nil {
		p.cleaner(object)
	}
	if p.metrics {
		p.inFlight.Add(-1)
	}

	if p.mode == ModeGCAware {
		p.gcInner.Put(object)
		return
	}

	pid := runtimeProcPin()
	shardID := safeconv.IntToUint32Truncate(pid) & p.shardMask
	s := &p.shards[shardID]

	if s.slot.tryPut(object) {
		runtimeProcUnpin()
		return
	}
	runtimeProcUnpin()
	s.pushFree(object)
}

// InFlight returns the number of objects handed out by Get() but not yet returned by
// Put().
//
// Returns int64 which is the current in-flight count, or zero when metrics are disabled
// because WithMetrics() was not enabled. Opt-in to avoid the per-call atomic add on the
// hot path.
func (p *Pool[T]) InFlight() int64 {
	return p.inFlight.Load()
}

// Close releases pool resources. No background goroutines exist, so this is a no-op.
func (*Pool[T]) Close() {}

// Cap returns the current total slab capacity for modePersistent. In ModeGCAware the
// result is zero because sync.Pool does not expose its cache size.
//
// Returns int which is the persistent slab capacity in objects, or zero in ModeGCAware.
//
// Concurrency: Safe for concurrent use. Guarded by slabsMu.
func (p *Pool[T]) Cap() int {
	if p.mode == ModeGCAware {
		return 0
	}
	p.slabsMu.Lock()
	defer p.slabsMu.Unlock()
	return p.capacity
}

// MaxCap returns the configured growth ceiling.
//
// Returns int which is the maximum capacity in objects, or zero when growth is disabled.
func (p *Pool[T]) MaxCap() int { return p.maxCapacity }

// Drain removes every object from the pool and returns them in unspecified order.
// Intended for shutdown and testing.
//
// MUST NOT be called concurrently with Get() or Put(). In ModeGCAware the returned slice
// is best-effort because sync.Pool does not expose drain semantics.
//
// Returns []*T which contains the objects removed from the pool, or an empty slice when
// the pool is already drained.
func (p *Pool[T]) Drain() []*T {
	if p.mode == ModeGCAware {
		return p.drainGCInner()
	}

	out := make([]*T, 0, p.capacity)
	for i := range p.shards {
		if object := p.shards[i].slot.drainSlot(); object != nil {
			out = append(out, object)
		}
	}
	for i := range p.shards {
		for {
			object := p.shards[i].popFree()
			if object == nil {
				break
			}
			out = append(out, object)
		}
	}
	return out
}

// bootstrapStorage initialises per-mode storage: allocates the first slab and shards for
// modePersistent, or builds and pre-warms the inner sync.Pool for ModeGCAware.
//
// Takes poolConfig (config[T]) which is the resolved configuration.
// Takes initialise (func(*T)) which is the per-object constructor used by the inner
// sync.Pool.
// Takes capacity (int) which is the initial capacity to pre-warm.
//
// Returns error which is nil on success, or the error from the initial slab allocation.
func (p *Pool[T]) bootstrapStorage(poolConfig config[T], initialise func(*T), capacity int) error {
	switch poolConfig.mode {
	case modePersistent:
		shardN := nextPow2(poolConfig.shards)
		p.shards = make([]shard[T], shardN)
		p.shardMask = safeconv.IntToUint32(shardN - 1)
		return p.appendSlab(capacity, 0)
	case ModeGCAware:
		p.gcInner = &sync.Pool{
			New: func() any {
				p.gcFresh.Add(1)
				var object T
				initialise(&object)
				return &object
			},
		}
		p.prewarmGCInner(capacity)
	}
	return nil
}

// prewarmGCInner allocates capacity objects so the inner sync.Pool per-P caches are
// populated. Objects may be reclaimed by the next GC but still reduce first-request
// latency.
//
// Takes capacity (int) which is the number of objects to pre-allocate and return to the
// pool.
func (p *Pool[T]) prewarmGCInner(capacity int) {
	warm := make([]*T, capacity)
	for i := range warm {
		warm[i] = p.popGCInner()
	}
	for _, object := range warm {
		p.gcInner.Put(object)
	}
}

// popGCInner pops one object from the inner sync.Pool and asserts its type.
//
// Returns *T which is the popped object, or nil when sync.Pool itself returns nil.
func (p *Pool[T]) popGCInner() *T {
	value := p.gcInner.Get()
	if value == nil {
		return nil
	}
	if object, ok := value.(*T); ok {
		return object
	}
	object := new(T)
	p.initialise(object)
	return object
}

// drainGCInner pulls cached objects out of the inner sync.Pool until New has produced
// gcAwareDrainAttempts fresh ones, which are dropped to bound the loop.
//
// Returns []*T which contains the cached objects removed from the pool.
func (p *Pool[T]) drainGCInner() []*T {
	out := make([]*T, 0)
	var fresh int
	for fresh < gcAwareDrainAttempts {
		before := p.gcFresh.Load()
		object := p.popGCInner()
		if object == nil {
			return out
		}
		if p.gcFresh.Load() != before {
			fresh++
			continue
		}
		out = append(out, object)
	}
	return out
}

// appendSlab allocates one slab of individually heap-allocated objects and distributes
// them across shards starting at preferShard, sidestepping the GC bitmap pessimisation
// for arrays of large pointer-bearing values.
//
// Takes size (int) which is the number of objects to allocate in this slab.
// Takes preferShard (int) which is the shard index that receives the first object.
// Further objects round-robin from there.
//
// Returns error which is nil on success.
func (p *Pool[T]) appendSlab(size int, preferShard int) error {
	objects := make([]*T, size)
	for i := range objects {
		objects[i] = new(T)
		p.initialise(objects[i])
	}
	p.slabs = append(p.slabs, objects)
	p.capacity += size

	mask := safeconv.IntToUint32(len(p.shards) - 1)
	for i := range objects {
		shardIdx := safeconv.IntToUint32Truncate(preferShard+i) & mask
		p.shards[shardIdx].pushFree(objects[i])
	}
	return nil
}

// getPersistent is the modePersistent fast path for Get, split out so metric accounting
// does not inflate the inlined body.
//
// Returns *T which is a pooled object, or nil when every shard is empty and growth is
// exhausted.
func (p *Pool[T]) getPersistent() *T {
	pid := runtimeProcPin()
	shardID := safeconv.IntToUint32Truncate(pid) & p.shardMask
	s := &p.shards[shardID]

	if object := s.slot.tryTake(); object != nil {
		runtimeProcUnpin()
		return object
	}

	runtimeProcUnpin()
	if object := s.popFree(); object != nil {
		return object
	}

	if object := p.steal(int(shardID)); object != nil {
		return object
	}
	return p.grow(int(shardID))
}

// takeParked takes the object parked in the calling P's slot, or else the first object
// parked in another shard's slot, walking shards in increasing order.
//
// Returns *T which is the parked object, or nil when every slot is empty.
func (p *Pool[T]) takeParked() *T {
	pid := runtimeProcPin()
	shardID := safeconv.IntToUint32Truncate(pid) & p.shardMask
	object := p.shards[shardID].slot.tryTake()
	runtimeProcUnpin()
	if object != nil {
		return object
	}
	for offset := uint32(1); offset <= p.shardMask; offset++ {
		slot := &p.shards[(shardID+offset)&p.shardMask].slot
		if !slot.holds() {
			continue
		}
		if object := slot.tryTake(); object != nil {
			return object
		}
	}
	return nil
}

// steal walks neighbour shards in increasing order from start+1 and returns the first
// object found.
//
// Takes start (int) which is the index of the originating shard. The walk skips this
// shard.
//
// Returns *T which is the first object popped from a neighbour shard, or nil when every
// other shard is empty.
func (p *Pool[T]) steal(start int) *T {
	numShards := len(p.shards)
	mask := safeconv.IntToUint32(numShards - 1)
	for i := 1; i < numShards; i++ {
		index := safeconv.IntToUint32Truncate(start+i) & mask
		if object := p.shards[index].popFree(); object != nil {
			return object
		}
	}
	return nil
}

// grow allocates a new slab, distributes its objects across shards starting at
// preferShard, and returns one. Safe for concurrent use; serialised by slabsMu.
//
// Takes preferShard (int) which is the shard that receives the first slot of the new
// slab.
//
// Returns *T which is a freshly initialised object when growth succeeds, or nil when
// growth is disabled or the maxCapacity bound is reached.
func (p *Pool[T]) grow(preferShard int) *T {
	if p.maxCapacity == 0 {
		return nil
	}

	p.slabsMu.Lock()
	defer p.slabsMu.Unlock()

	if object := p.steal(preferShard); object != nil {
		return object
	}
	if object := p.shards[preferShard].popFree(); object != nil {
		return object
	}

	if p.capacity >= p.maxCapacity {
		return nil
	}

	chunk := min(p.capacity, p.maxCapacity-p.capacity)
	if chunk <= 0 {
		return nil
	}
	if err := p.appendSlab(chunk, preferShard); err != nil {
		return nil
	}

	if object := p.shards[preferShard].popFree(); object != nil {
		return object
	}
	return p.steal(preferShard)
}

// config holds the resolved construction parameters and is not exposed publicly.
type config[T any] struct {
	// shards holds the requested number of shards before rounding to a power of two.
	shards int

	// maxCapacity holds the growth ceiling. Zero disables growth.
	maxCapacity int

	// mode selects the storage and reclamation strategy.
	mode mode

	// metrics enables the in-flight counter when true.
	metrics bool
}

// WithMode selects the storage strategy.
//
// Takes m (mode) which is the storage strategy to install on the Pool. Defaults to
// modePersistent.
//
// Returns Option[T] which records the chosen mode in the configuration.
func WithMode[T any](m mode) Option[T] {
	return func(c *config[T]) { c.mode = m }
}

// WithShardCount overrides the number of shards from the default runtime.GOMAXPROCS(0).
// The actual count is rounded up to the next power of two.
//
// Takes n (int) which is the requested shard count before rounding.
//
// Returns Option[T] which records the requested shard count in the configuration.
func WithShardCount[T any](n int) Option[T] {
	return func(c *config[T]) { c.shards = n }
}

// WithGrowth permits the pool to allocate additional slabs on demand. Pass 0 to disable.
//
// Takes maxCapacity (int) which is the upper bound on total pooled objects.
//
// Returns Option[T] which records the growth ceiling in the configuration.
func WithGrowth[T any](maxCapacity int) Option[T] {
	return func(c *config[T]) { c.maxCapacity = maxCapacity }
}

// WithMetrics enables the in-flight counter queryable via Pool.InFlight, at the cost of
// one atomic add per Get and Put.
//
// Returns Option[T] which records the metrics opt-in in the configuration.
func WithMetrics[T any]() Option[T] {
	return func(c *config[T]) { c.metrics = true }
}

// FactoryInit adapts a func() *T factory to the in-place initialise form expected by New.
// Prefer a direct in-place initialiser when the extra allocation matters.
//
// Takes factory (func() *T) which is a constructor returning a freshly allocated *T.
//
// Returns func(*T) which is suitable for use as the initialise argument to New().
func FactoryInit[T any](factory func() *T) func(*T) {
	return func(destination *T) { *destination = *factory() }
}

// validateLinkPlacement reflect-checks that T's first declared field is stablepool.Link,
// running once at New.
//
// Returns error which is ErrBadLink wrapped with the offending type detail when the
// placement is wrong, otherwise nil.
func validateLinkPlacement[T any]() error {
	var t T
	typ := reflect.TypeOf(t)
	if typ == nil || typ.Kind() != reflect.Struct {
		return fmt.Errorf("%w: T is not a struct", ErrBadLink)
	}
	if typ.NumField() == 0 {
		return fmt.Errorf("%w: T has no fields", ErrBadLink)
	}
	field := typ.Field(0)
	if field.Type != reflect.TypeFor[Link]() {
		return fmt.Errorf("%w: first field of %s is %s at offset %d (expected stablepool.Link at 0)",
			ErrBadLink, typ.String(), field.Type, field.Offset)
	}
	return nil
}

// nextPow2 returns the smallest power of two greater than or equal to n.
//
// Takes n (int) which is a non-negative integer to round up.
//
// Returns int which is the smallest power of two greater than or equal to n. Returns 1
// when n is at most 1.
func nextPow2(n int) int {
	if n <= 1 {
		return 1
	}
	return 1 << bits.Len(uint(n-1))
}

// resolvePoolConfig validates the public constructor arguments and merges caller-supplied
// options.
//
// Takes initialise (func(*T)) which is the constructor passed to New(). Must be non-nil.
// Takes capacity (int) which is the requested initial capacity. Must be positive.
// Takes options ([]Option[T]) which are zero or more Option values applied after
// defaults.
//
// Returns config[T] which is the resolved configuration when every value passes
// validation.
// Returns error which is ErrNoInitialiser, ErrBadCapacity, ErrBadShardCount, or
// ErrBadGrowth on invalid input.
func resolvePoolConfig[T any](initialise func(*T), capacity int, options []Option[T]) (config[T], error) {
	if initialise == nil {
		return config[T]{}, ErrNoInitialiser
	}
	if capacity <= 0 {
		return config[T]{}, ErrBadCapacity
	}
	poolConfig := config[T]{shards: runtime.GOMAXPROCS(0), maxCapacity: 0, mode: 0, metrics: false}
	for _, opt := range options {
		opt(&poolConfig)
	}
	if poolConfig.shards <= 0 {
		return config[T]{}, ErrBadShardCount
	}
	if poolConfig.maxCapacity > 0 && poolConfig.maxCapacity < capacity {
		return config[T]{}, ErrBadGrowth
	}
	return poolConfig, nil
}
