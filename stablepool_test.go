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

package stablepool_test

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"piko.sh/stablepool"
)

type buffer struct {
	stablepool.Link
	id   int
	data []byte
}

type misplacedLink struct {
	id int
	stablepool.Link
}

type noLink struct {
	id int
}

func newCounterInit() (func(*buffer), *atomic.Int64) {
	var next atomic.Int64
	return func(b *buffer) {
		b.id = int(next.Add(1))
		b.data = make([]byte, 0, 8)
	}, &next
}

const takeAllLimit = 1024

func takeAll(t *testing.T, p *stablepool.Pool[buffer]) []*buffer {
	t.Helper()
	out := make([]*buffer, 0, 16)
	for len(out) < takeAllLimit {
		object := p.Get()
		if object == nil {
			return out
		}
		out = append(out, object)
	}
	require.Failf(t, "the pool never runs dry", "Get handed out %d objects without reporting nil", takeAllLimit)
	return nil
}

func TestNewRejectsBadConfiguration(t *testing.T) {
	t.Parallel()
	init, _ := newCounterInit()
	tests := []struct {
		name    string
		build   func() error
		wantErr error
	}{
		{
			name:    "missing initialiser",
			build:   func() error { _, err := stablepool.New[buffer](nil, nil, 4); return err },
			wantErr: stablepool.ErrNoInitialiser,
		},
		{
			name:    "zero capacity",
			build:   func() error { _, err := stablepool.New(init, nil, 0); return err },
			wantErr: stablepool.ErrBadCapacity,
		},
		{
			name:    "negative capacity",
			build:   func() error { _, err := stablepool.New(init, nil, -3); return err },
			wantErr: stablepool.ErrBadCapacity,
		},
		{
			name:    "zero shard count",
			build:   func() error { _, err := stablepool.New(init, nil, 4, stablepool.WithShardCount[buffer](0)); return err },
			wantErr: stablepool.ErrBadShardCount,
		},
		{
			name: "negative shard count",
			build: func() error {
				_, err := stablepool.New(init, nil, 4, stablepool.WithShardCount[buffer](-1))
				return err
			},
			wantErr: stablepool.ErrBadShardCount,
		},
		{
			name:    "growth ceiling below capacity",
			build:   func() error { _, err := stablepool.New(init, nil, 4, stablepool.WithGrowth[buffer](2)); return err },
			wantErr: stablepool.ErrBadGrowth,
		},
		{
			name:    "link not first",
			build:   func() error { _, err := stablepool.New(func(*misplacedLink) {}, nil, 4); return err },
			wantErr: stablepool.ErrBadLink,
		},
		{
			name:    "link absent",
			build:   func() error { _, err := stablepool.New(func(*noLink) {}, nil, 4); return err },
			wantErr: stablepool.ErrBadLink,
		},
		{
			name:    "no fields",
			build:   func() error { _, err := stablepool.New(func(*struct{}) {}, nil, 4); return err },
			wantErr: stablepool.ErrBadLink,
		},
		{
			name:    "not a struct",
			build:   func() error { _, err := stablepool.New(func(*int) {}, nil, 4); return err },
			wantErr: stablepool.ErrBadLink,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.ErrorIs(t, tt.build(), tt.wantErr)
		})
	}
}

func TestNewAcceptsLinklessTypesInGCAwareMode(t *testing.T) {
	t.Parallel()
	pool, err := stablepool.New(func(n *noLink) { n.id = 1 }, nil, 2, stablepool.WithMode[noLink](stablepool.ModeGCAware))
	require.NoError(t, err)
	object := pool.Get()
	require.NotNil(t, object)
	require.Equal(t, 1, object.id)
	pool.Put(object)
}

func TestGetPutHonoursCapacity(t *testing.T) {
	t.Parallel()
	init, counter := newCounterInit()
	pool, err := stablepool.New(init, nil, 4, stablepool.WithShardCount[buffer](1))
	require.NoError(t, err)
	require.Equal(t, int64(4), counter.Load(), "every slot is initialised up front")
	require.Equal(t, 4, pool.Cap())
	require.Equal(t, 0, pool.MaxCap())

	taken := takeAll(t, pool)
	require.Len(t, taken, 4)
	seen := make(map[int]bool, len(taken))
	for _, b := range taken {
		require.NotNil(t, b)
		require.Positive(t, b.id)
		require.False(t, seen[b.id], "objects must be handed out once each")
		seen[b.id] = true
	}
	require.Nil(t, pool.Get(), "a drained pool without growth returns nil")

	pool.Put(taken[0])
	require.Same(t, taken[0], pool.Get(), "a returned object is reused")
	require.Equal(t, int64(4), counter.Load(), "reuse never re-initialises")
}

func TestGetSpansShards(t *testing.T) {
	t.Parallel()
	init, _ := newCounterInit()
	pool, err := stablepool.New(init, nil, 10, stablepool.WithShardCount[buffer](3))
	require.NoError(t, err)
	require.Len(t, takeAll(t, pool), 10, "Get steals from every shard, whichever P it runs on")
}

func TestPutRunsCleanerAndIgnoresNil(t *testing.T) {
	t.Parallel()
	init, _ := newCounterInit()
	cleaned := 0
	clean := func(b *buffer) { cleaned++; b.data = b.data[:0] }
	pool, err := stablepool.New(init, clean, 1, stablepool.WithShardCount[buffer](1))
	require.NoError(t, err)

	object := pool.Get()
	require.NotNil(t, object)
	object.data = append(object.data, 1, 2, 3)
	pool.Put(object)
	require.Equal(t, 1, cleaned)
	require.Empty(t, object.data)

	pool.Put(nil)
	require.Equal(t, 1, cleaned, "a nil Put is ignored before the cleaner runs")
	require.Same(t, object, pool.Get())
}

func TestMustGetAllocatesWhenDrained(t *testing.T) {
	t.Parallel()
	init, counter := newCounterInit()
	pool, err := stablepool.New(init, nil, 1, stablepool.WithShardCount[buffer](1), stablepool.WithMetrics[buffer]())
	require.NoError(t, err)

	pooled := pool.MustGet()
	require.NotNil(t, pooled)
	require.Equal(t, int64(1), pool.InFlight())

	fresh := pool.MustGet()
	require.NotNil(t, fresh)
	require.NotSame(t, pooled, fresh)
	require.Equal(t, int64(2), counter.Load(), "the fallback object is initialised")
	require.Equal(t, int64(2), pool.InFlight())

	pool.Put(pooled)
	pool.Put(fresh)
	require.Equal(t, int64(0), pool.InFlight())
	require.Len(t, takeAll(t, pool), 2, "the fallback object joins the pool on Put")
}

func TestInFlightMetrics(t *testing.T) {
	t.Parallel()
	init, _ := newCounterInit()
	tests := []struct {
		name    string
		options []stablepool.Option[buffer]
		wantMax int64
	}{
		{name: "metrics enabled", options: []stablepool.Option[buffer]{stablepool.WithMetrics[buffer]()}, wantMax: 2},
		{name: "metrics disabled", options: nil, wantMax: 0},
		{name: "metrics enabled gc-aware", options: []stablepool.Option[buffer]{stablepool.WithMetrics[buffer](), stablepool.WithMode[buffer](stablepool.ModeGCAware)}, wantMax: 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			pool, err := stablepool.New(init, nil, 4, tt.options...)
			require.NoError(t, err)
			require.Equal(t, int64(0), pool.InFlight())
			a := pool.Get()
			b := pool.Get()
			require.NotNil(t, a)
			require.NotNil(t, b)
			require.Equal(t, tt.wantMax, pool.InFlight())
			pool.Put(a)
			pool.Put(b)
			require.Equal(t, int64(0), pool.InFlight())
		})
	}
}

func TestGrowthAppendsSlabsUpToTheCeiling(t *testing.T) {
	t.Parallel()
	init, counter := newCounterInit()
	pool, err := stablepool.New(init, nil, 2, stablepool.WithShardCount[buffer](1), stablepool.WithGrowth[buffer](7))
	require.NoError(t, err)
	require.Equal(t, 2, pool.Cap())
	require.Equal(t, 7, pool.MaxCap())

	first := pool.Get()
	second := pool.Get()
	require.NotNil(t, first)
	require.NotNil(t, second)
	require.Equal(t, 2, pool.Cap(), "no growth while objects remain")

	third := pool.Get()
	require.NotNil(t, third, "exhaustion triggers growth")
	require.Equal(t, 4, pool.Cap(), "the slab doubles")
	require.Equal(t, int64(4), counter.Load())

	taken := make([]*buffer, 0, 7)
	taken = append(taken, first, second, third)
	taken = append(taken, takeAll(t, pool)...)
	require.Len(t, taken, 7, "growth stops at MaxCap")
	require.Equal(t, 7, pool.Cap())
	require.Nil(t, pool.Get())

	for _, b := range taken {
		pool.Put(b)
	}
	require.Len(t, takeAll(t, pool), 7)
	require.Equal(t, 7, pool.Cap(), "Put never grows the slab")
}

func TestGrowthAcrossShards(t *testing.T) {
	t.Parallel()
	init, _ := newCounterInit()
	pool, err := stablepool.New(init, nil, 3, stablepool.WithShardCount[buffer](4), stablepool.WithGrowth[buffer](9))
	require.NoError(t, err)
	require.Len(t, takeAll(t, pool), 9)
	require.Equal(t, 9, pool.Cap())
}

func TestDrainReturnsEveryObjectOnce(t *testing.T) {
	t.Parallel()
	init, _ := newCounterInit()
	pool, err := stablepool.New(init, nil, 5, stablepool.WithShardCount[buffer](2))
	require.NoError(t, err)

	held := pool.Get()
	require.NotNil(t, held)
	pool.Put(held)

	drained := pool.Drain()
	require.Len(t, drained, 5)
	seen := make(map[*buffer]bool, len(drained))
	for _, b := range drained {
		require.False(t, seen[b])
		seen[b] = true
	}
	require.Empty(t, pool.Drain(), "a second Drain finds nothing")
	require.Nil(t, pool.Get())

	for _, b := range drained {
		pool.Put(b)
	}
	require.Len(t, takeAll(t, pool), 5, "drained objects can be returned to refill the pool")
}

func TestGCAwareMode(t *testing.T) {
	t.Parallel()
	init, counter := newCounterInit()
	pool, err := stablepool.New(init, nil, 3, stablepool.WithMode[buffer](stablepool.ModeGCAware))
	require.NoError(t, err)
	require.GreaterOrEqual(t, counter.Load(), int64(3), "pre-warm initialises the requested capacity")
	require.Equal(t, 0, pool.Cap(), "sync.Pool does not expose a capacity")
	require.Equal(t, 0, pool.MaxCap())

	for range 8 {
		object := pool.Get()
		require.NotNil(t, object, "GC-aware Get allocates rather than returning nil")
		require.Positive(t, object.id)
		pool.Put(object)
	}
	require.NotNil(t, pool.MustGet())

	drained := pool.Drain()
	for _, b := range drained {
		require.NotNil(t, b)
	}
	require.NotNil(t, pool.Get(), "Get still succeeds after Drain")
}

func TestFactoryInit(t *testing.T) {
	t.Parallel()
	calls := 0
	factory := func() *buffer {
		calls++
		return &buffer{Link: stablepool.Link{}, id: 7, data: []byte("seed")}
	}
	init := stablepool.FactoryInit(factory)

	var b buffer
	init(&b)
	require.Equal(t, 7, b.id)
	require.Equal(t, []byte("seed"), b.data)
	require.Equal(t, 1, calls)

	pool, err := stablepool.New(init, nil, 3, stablepool.WithShardCount[buffer](1))
	require.NoError(t, err)
	require.Equal(t, 4, calls, "one factory call per slot")
	for _, object := range takeAll(t, pool) {
		require.Equal(t, 7, object.id)
	}
}

func TestWithShardCountRoundsUpToPowerOfTwo(t *testing.T) {
	t.Parallel()
	init, _ := newCounterInit()
	for _, shards := range []int{1, 2, 3, 5, 8, 9} {
		t.Run(fmt.Sprintf("%d shards", shards), func(t *testing.T) {
			t.Parallel()
			pool, err := stablepool.New(init, nil, 16, stablepool.WithShardCount[buffer](shards))
			require.NoError(t, err)
			require.Len(t, pool.Drain(), 16, "every object is handed out once")
		})
	}
}

func TestCloseIsANoOp(t *testing.T) {
	t.Parallel()
	init, _ := newCounterInit()
	pool, err := stablepool.New(init, nil, 2)
	require.NoError(t, err)
	pool.Close()
	require.NotNil(t, pool.Get())
	pool.Close()
}

func TestConcurrentGetPut(t *testing.T) {
	t.Parallel()
	const (
		goroutines = 8
		iterations = 2000
		capacity   = 64
	)
	init, _ := newCounterInit()
	pool, err := stablepool.New(init, nil, capacity, stablepool.WithShardCount[buffer](4), stablepool.WithMetrics[buffer]())
	require.NoError(t, err)

	var misses atomic.Int64
	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Go(func() {
			for i := range iterations {
				object := pool.Get()
				if object == nil {
					misses.Add(1)
					continue
				}
				object.data = append(object.data[:0], byte(g), byte(i))
				pool.Put(object)
			}
		})
	}
	wg.Wait()

	require.Equal(t, int64(0), misses.Load(), "capacity exceeds the number of goroutines so Get never misses")
	require.Equal(t, int64(0), pool.InFlight())
	require.Len(t, pool.Drain(), capacity, "every object is back in the pool")
}
