package mem

import (
	"runtime/debug"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/apache/arrow/go/v17/arrow/memory"
)

/*
#cgo LDFLAGS: -lstdc++
#include <stdlib.h>

void* malloc(size_t size);
void* realloc(void* ptr, size_t size);
void free(void* ptr);
*/
import "C"

// CGoAllocator is a memory.Allocator that uses C's malloc, realloc, and free.
var CGoAllocator memory.Allocator = &cgoAllocator{}

type cgoAllocator struct{}

func (cgoAllocator) Allocate(size int) []byte {
	return unsafe.Slice((*byte)(C.malloc(C.size_t(size))), size)
}

func (cgoAllocator) Free(b []byte) {
	C.free(unsafe.Pointer(&b[0]))
}

func (cgoAllocator) Reallocate(size int, b []byte) []byte {
	return unsafe.Slice((*byte)(C.realloc(unsafe.Pointer(&b[0]), C.size_t(size))), size)
}

var _ memory.Allocator = (*LeakCheckAllocator)(nil)

// LeakCheckAllocator is a memory allocator that tracks memory
// allocations and can be used to check for memory leaks.
type LeakCheckAllocator struct {
	inner memory.Allocator

	lock    sync.Mutex
	tracker map[*byte]claim

	allocations, reallocations, frees atomic.Uint64
}

type claim struct {
	area  []byte
	stack string
}

func (l *LeakCheckAllocator) Allocate(size int) []byte {
	l.allocations.Add(1)
	l.lock.Lock()
	defer l.lock.Unlock()

	mem := l.inner.Allocate(size)
	l.tracker[&mem[0]] = claim{
		mem,
		string(debug.Stack()),
	}

	return mem
}

func (l *LeakCheckAllocator) Reallocate(size int, b []byte) []byte {
	l.reallocations.Add(1)
	l.lock.Lock()
	defer l.lock.Unlock()

	if _, ok := l.tracker[&b[0]]; ok {
		delete(l.tracker, &b[0])
	} else {
		panic("reallocating unknown memory")
	}

	mem := l.inner.Reallocate(size, b)
	l.tracker[&mem[0]] = claim{
		mem,
		string(debug.Stack()),
	}

	return mem
}

func (l *LeakCheckAllocator) Free(b []byte) {
	l.frees.Add(1)
	l.lock.Lock()
	defer l.lock.Unlock()

	delete(l.tracker, &b[0])
	l.inner.Free(b)
}

func (l *LeakCheckAllocator) Tracker() map[*byte]claim {
	return l.tracker
}

func (l *LeakCheckAllocator) Allocations() uint64 {
	return l.allocations.Load()
}

func (l *LeakCheckAllocator) Reallocations() uint64 {
	return l.reallocations.Load()
}

func (l *LeakCheckAllocator) Frees() uint64 {
	return l.frees.Load()
}

// NewLeakCheckAllocator creates a new allocator with the ability of
// tracking memory requests. This allocator is useful alongside the
// Stacks() method which returns stack traces for the currently
// chunks of memory.
//
// ! This allocator should be used for debug memory usage and it's
// ! not suited for production usage.
func NewLeakCheckAllocator(inner memory.Allocator) *LeakCheckAllocator {
	return &LeakCheckAllocator{
		inner:   inner,
		tracker: make(map[*byte]claim),
	}
}

type TestingT interface {
	Errorf(format string, args ...any)
}

// CheckAllocatorLeaks checks for memory leaks in the allocator and reports
// them to the testing framework.
func CheckAllocatorLeaks(t TestingT, allocator *LeakCheckAllocator, includeStack bool) {
	allocator.lock.Lock()
	defer allocator.lock.Unlock()

	if len(allocator.tracker) == 0 {
		// keep the exit code from the test run
		return
	}

	t.Errorf("allocator: LeakCheckAllocator detected %d leaks:\n", len(allocator.tracker))

	if includeStack {
		for ptr, value := range allocator.tracker {
			t.Errorf("%p) %s\n", ptr, value.stack)
		}
	}

	// print allocation stats
	t.Errorf("\nallocator: allocations: %d, reallocations: %d, frees: %d\n",
		allocator.Allocations(),
		allocator.Reallocations(),
		allocator.Frees(),
	)
}
