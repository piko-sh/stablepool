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
	"sync/atomic"
)

// privateSlot holds the per-shard fast-path object. Atomic because multiple Ps can share
// a shard, and a plain slot would hand the same object to two goroutines.
type privateSlot[T any] struct {
	// p holds the per-P pooled object pointer, accessed atomically.
	p atomic.Pointer[T]
}

// tryTake atomically removes and returns the slot's current value, or nil.
//
// Returns *T which is the previously stored pointer, or nil when the slot was empty.
func (s *privateSlot[T]) tryTake() *T {
	return s.p.Swap(nil)
}

// holds reports whether the slot currently holds an object, without the write a swap
// makes to the slot's cache line.
//
// Returns bool which is true when the slot was not empty when it was loaded.
func (s *privateSlot[T]) holds() bool {
	return s.p.Load() != nil
}

// tryPut stores object into the slot when the slot is empty.
//
// Takes object (*T) which is the pointer to attempt to store in the slot.
//
// Returns bool which is true when object was stored, false when the slot already held a
// value.
func (s *privateSlot[T]) tryPut(object *T) bool {
	return s.p.CompareAndSwap(nil, object)
}

// drainSlot atomically clears the slot and returns its previous value.
//
// Returns *T which is the previously stored pointer, or nil when the slot was empty.
func (s *privateSlot[T]) drainSlot() *T {
	return s.p.Swap(nil)
}
