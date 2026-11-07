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

//go:build safe || (js && wasm)

package stablepool

import (
	"sync"
)

// shard is one per-P cache region (safe build). Uses a []*T slice protected by freeMu
// instead of intrusive linking.
type shard[T any] struct {
	// slot is the shard's single-object fast path, atomic because several Ps share a shard.
	slot privateSlot[T]

	// freeStack is the overflow stack protected by freeMu.
	freeStack []*T

	// freeMu guards freeStack.
	freeMu sync.Mutex
}

// popFree removes and returns the last element of this shard's free stack.
//
// Returns *T which is the detached object, or nil when the stack is empty.
//
// Concurrency: Acquires freeMu for the duration of the operation.
func (s *shard[T]) popFree() *T {
	s.freeMu.Lock()
	if len(s.freeStack) == 0 {
		s.freeMu.Unlock()
		return nil
	}
	last := len(s.freeStack) - 1
	object := s.freeStack[last]
	s.freeStack[last] = nil
	s.freeStack = s.freeStack[:last]
	s.freeMu.Unlock()
	return object
}

// pushFree appends object to the shard's free stack under freeMu.
//
// Takes object (*T) which is the object to push onto the free stack.
//
// Concurrency: Acquires freeMu for the duration of the operation.
func (s *shard[T]) pushFree(object *T) {
	s.freeMu.Lock()
	s.freeStack = append(s.freeStack, object)
	s.freeMu.Unlock()
}
