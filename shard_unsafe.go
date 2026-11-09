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

//go:build go1.27 && !safe && !(js && wasm)

package stablepool

import (
	"sync"
	"sync/atomic"
	"unsafe"
)

// shard is one per-P cache region, holding a lock-free private slot for the Ps mapped to
// it and a locked overflow stack that any P can steal from. It is padded to 128 bytes so
// neighbouring shards never false-share.
type shard[T any] struct {
	// slot is the shard's single-object fast path, atomic because several Ps share a shard.
	slot privateSlot[T]

	// freeHead is the stack of overflow objects. It is an atomic.Pointer rather than a
	// counted uint64 so the GC traces the chain, because untraced objects would be reclaimed
	// while still linked.
	freeHead atomic.Pointer[T]

	// freeMu guards freeHead and the intrusive next links of the objects on it. A CAS
	// Treiber stack would suffer ABA here and could link an in-use object into the list.
	freeMu sync.Mutex

	// freeCount is the number of objects on the free stack, guarded by freeMu.
	freeCount int

	_ [128 - 32]byte
}

// popFree removes and returns the head of this shard's free stack.
//
// Runs under freeMu. The next pointer is still read with atomic.LoadPointer() so the race
// detector sees the push-then-pop chain.
//
// Returns *T which is the detached head object, or nil when the stack is empty.
//
// Concurrency: Acquires freeMu for the duration of the operation.
//
//go:nocheckptr
func (s *shard[T]) popFree() *T {
	s.freeMu.Lock()
	head := s.freeHead.Load()
	if head == nil {
		s.freeMu.Unlock()
		return nil
	}
	next := (*T)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(head))))
	s.freeHead.Store(next)
	s.freeCount--
	s.freeMu.Unlock()
	return head
}

// pushFree prepends object to the shard's free stack under freeMu. The intrusive
// Link.next at offset 0 of T is written via atomic.StorePointer() so the race detector
// sees the happens-before edge to the popFree() that later reads it.
//
// Takes object (*T) which is the object to push onto the free stack head.
// Takes limit (int) which is the most objects the stack may hold; zero means no limit.
//
// Returns bool which is false when the stack is full and the object was not pushed.
//
// Concurrency: Acquires freeMu for the duration of the operation.
//
//go:nocheckptr
func (s *shard[T]) pushFree(object *T, limit int) bool {
	s.freeMu.Lock()
	if limit > 0 && s.freeCount >= limit {
		s.freeMu.Unlock()
		return false
	}
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(object)), unsafe.Pointer(s.freeHead.Load()))
	s.freeHead.Store(object)
	s.freeCount++
	s.freeMu.Unlock()
	return true
}
