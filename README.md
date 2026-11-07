# stablepool

A **pre-warmed, sharded object pool whose contents survive garbage collection**, for Go.

`sync.Pool` empties itself on every GC cycle. Objects that carry allocated state (buffers,
maps, builders) are then rebuilt on the next `Get`, so latency spikes after each collection.
`stablepool` commits its capacity up front in pool-owned slabs and keeps every object across
GC cycles: no first-call allocation, no post-GC cliff, no surprise re-initialisation. In
exchange its memory footprint is the capacity you ask for; it does not shrink under memory
pressure unless you choose `ModeGCAware`.

Module `piko.sh/stablepool`. Go 1.27. Apache-2.0. Depends only on `piko.sh/safeconv`.

## Install

```sh
go get piko.sh/stablepool
```

## Usage

```go
package main

import (
	"fmt"

	"piko.sh/stablepool"
)

type buffer struct {
	stablepool.Link // must be the first field
	data []byte
}

func main() {
	pool, err := stablepool.New[buffer](
		func(b *buffer) { b.data = make([]byte, 0, 4096) }, // initialise once, in place
		func(b *buffer) { b.data = b.data[:0] },            // clean on Put
		1024,                                               // capacity, committed now
		stablepool.WithGrowth[buffer](4096),
	)
	if err != nil {
		panic(err)
	}

	b := pool.MustGet()
	b.data = append(b.data, "hello"...)
	fmt.Println(string(b.data))
	pool.Put(b)
}
```

## What it provides

- **`New[T]`** builds a pool of `capacity` objects, each initialised in place. `T` must embed
  `stablepool.Link` as its first field; the pool uses that word for its free list while the
  object is pooled and never touches it while the object is in your hands.
- **`Get`** returns a pooled object or nil when the pool is empty and cannot grow;
  **`MustGet`** allocates a fresh object instead of returning nil; **`GetPreferParked`**
  takes an object parked in any processor's slot first. **`Put`** runs the clean function and
  returns the object.
- **Options**: `WithGrowth` (extra slabs on demand up to a ceiling), `WithShardCount`
  (default `GOMAXPROCS`, rounded up to a power of two), `WithMetrics` (an in-flight counter,
  read with `InFlight`), and `WithMode(ModeGCAware)` for `sync.Pool` reclamation behind the
  same typed API.
- **`Cap`**, **`MaxCap`** and **`Drain`** report and empty the pool.
- **Sentinel errors** for invalid construction: `ErrNoInitialiser`, `ErrBadCapacity`,
  `ErrBadGrowth`, `ErrBadShardCount` and `ErrBadLink`.

`Get` and `Put` are safe for any number of goroutines. Each takes a per-processor fast slot,
then a sharded, ABA-safe Treiber free stack, then steals from other shards; growth takes a
single mutex and only happens when every shard is empty.

## Build tags

The default build pins the goroutine to its processor through `runtime.procPin` and uses
non-atomic slot access (atomic under `-race`). Build with `-tags safe`, or for
`GOOS=js GOARCH=wasm`, to drop `unsafe` and the runtime links in favour of mutex-guarded
shards with the same API.

## Development

```sh
go test -race ./...
go test -tags safe ./...
GOOS=js GOARCH=wasm go vet ./...
golangci-lint run
golangci-lint run --build-tags safe
```

stablepool was originally developed as part of [Pipit](https://github.com/piko-sh/pipit).
