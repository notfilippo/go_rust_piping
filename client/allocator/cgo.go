package allocator

import (
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
