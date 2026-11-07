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
	"unsafe"
)

// Link is the intrusive free-list header embedded as the first field of pooled types,
// required in the first-field position for modePersistent pools but not for ModeGCAware.
type Link struct {
	// next holds the successor pointer when this object sits on a shard free stack.
	next unsafe.Pointer //nolint:unused // used via unsafe.Pointer
}
