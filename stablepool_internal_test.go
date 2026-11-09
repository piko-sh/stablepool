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
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"piko.sh/safeconv"
)

type parkedObject struct {
	Link
	inUse atomic.Bool
	warm  bool
}

func newParkedPool(t *testing.T, options ...Option[parkedObject]) (*Pool[parkedObject], uint32) {
	t.Helper()
	previous := runtime.GOMAXPROCS(1)
	t.Cleanup(func() { runtime.GOMAXPROCS(previous) })
	options = append(options, WithShardCount[parkedObject](4))
	pool, err := New(func(*parkedObject) {}, nil, 4, options...)
	require.NoError(t, err)
	own := safeconv.IntToUint32Truncate(runtimeProcPin()) & pool.shardMask
	runtimeProcUnpin()
	return pool, own
}

func TestGetPreferParkedTakesAnObjectParkedOnAnotherShard(t *testing.T) {
	pool, own := newParkedPool(t, WithMetrics[parkedObject]())
	warm := &parkedObject{warm: true}
	require.True(t, pool.shards[(own+2)&pool.shardMask].slot.tryPut(warm))

	require.Same(t, warm, pool.GetPreferParked(), "the parked object is taken before the shard's cold free stack")
	require.Equal(t, int64(1), pool.InFlight())
	require.Nil(t, pool.shards[(own+2)&pool.shardMask].slot.p.Load(), "the other shard's slot is emptied")
}

func TestGetPreferParkedTakesItsOwnSlotFirst(t *testing.T) {
	pool, own := newParkedPool(t)
	mine := &parkedObject{warm: true}
	other := &parkedObject{warm: true}
	require.True(t, pool.shards[own].slot.tryPut(mine))
	require.True(t, pool.shards[(own+1)&pool.shardMask].slot.tryPut(other))

	require.Same(t, mine, pool.GetPreferParked())
	require.Same(t, other, pool.GetPreferParked())
}

func TestGetPreferParkedFallsBackToThePool(t *testing.T) {
	pool, _ := newParkedPool(t)
	for range 4 {
		object := pool.GetPreferParked()
		require.NotNil(t, object)
		require.False(t, object.warm, "with nothing parked, the free stacks serve")
	}
	require.NotNil(t, pool.GetPreferParked(), "an exhausted pool still allocates")
}

func TestGetPreferParkedInGCAwareModeIsMustGet(t *testing.T) {
	pool, err := New(func(object *parkedObject) { object.warm = true }, nil, 1, WithMode[parkedObject](ModeGCAware), WithMetrics[parkedObject]())
	require.NoError(t, err)
	require.NotNil(t, pool.GetPreferParked())
	require.Equal(t, int64(1), pool.InFlight())
}

func TestGetPreferParkedHandsEachObjectToOneTaker(t *testing.T) {
	pool, err := New(func(*parkedObject) {}, nil, 8, WithShardCount[parkedObject](8))
	require.NoError(t, err)
	var group sync.WaitGroup
	var doubled atomic.Int64
	for range 16 {
		group.Go(func() {
			for range 2000 {
				object := pool.GetPreferParked()
				if !object.inUse.CompareAndSwap(false, true) {
					doubled.Add(1)
				}
				runtime.Gosched()
				object.inUse.Store(false)
				pool.Put(object)
			}
		})
	}
	group.Wait()
	require.Zero(t, doubled.Load(), "no object is held by two takers at once")
}

func TestOverflowBoundsWhatTheShardsKeep(t *testing.T) {
	t.Parallel()
	pool, err := New(func(*parkedObject) {}, nil, 4, WithShardCount[parkedObject](1), WithOverflow[parkedObject]())
	require.NoError(t, err)
	require.Equal(t, 4, pool.shardLimit)

	taken := make([]*parkedObject, 0, 10)
	for range 10 {
		object := pool.Get()
		require.NotNil(t, object)
		taken = append(taken, object)
	}
	for _, object := range taken {
		pool.Put(object)
	}

	kept := 0
	if pool.shards[0].slot.tryTake() != nil {
		kept++
	}
	for pool.shards[0].popFree() != nil {
		kept++
	}
	require.LessOrEqual(t, kept, 1+pool.shardLimit, "the shard keeps its slot and at most its share; the rest go to the overflow")
	require.Equal(t, 5, kept)
}

func TestTryPutWalksToAShardWithRoom(t *testing.T) {
	pool, own := newParkedPool(t)
	require.Equal(t, 1, pool.shardLimit)
	emptyFreeStacks(pool)
	require.True(t, pool.shards[own].slot.tryPut(&parkedObject{}))
	require.True(t, pool.shards[own].pushFree(&parkedObject{}, 0))

	object := &parkedObject{}
	require.True(t, pool.TryPut(object), "another shard has room")
	require.Same(t, object, pool.shards[(own+1)&pool.shardMask].popFree())
}

func TestTryPutRefusesWhenEveryShardIsFull(t *testing.T) {
	pool, own := newParkedPool(t)
	require.True(t, pool.shards[own].slot.tryPut(&parkedObject{}))

	require.False(t, pool.TryPut(&parkedObject{}), "each shard holds its share of the capacity of four")
}

func TestOverflowWalksTheShardsBeforeTheOverflow(t *testing.T) {
	pool, own := newParkedPool(t, WithOverflow[parkedObject]())
	emptyFreeStacks(pool)
	require.True(t, pool.shards[own].slot.tryPut(&parkedObject{}))
	require.True(t, pool.shards[own].pushFree(&parkedObject{}, 0))

	object := &parkedObject{}
	pool.Put(object)
	require.Same(t, object, pool.shards[(own+1)&pool.shardMask].popFree(), "a slab object stays on the free stacks rather than going to the overflow")
}

func emptyFreeStacks(pool *Pool[parkedObject]) {
	for i := range pool.shards {
		for pool.shards[i].popFree() != nil {
		}
	}
}
