package client_test

import (
	"github.com/notfilippo/go_rust_piping/client"
	"github.com/notfilippo/go_rust_piping/client/mem"
	"testing"
)

func TestClient(t *testing.T) {
	allocator := mem.NewLeakCheckAllocator(mem.CGoAllocator)
	defer mem.CheckAllocatorLeaks(t, allocator, false)

	client.Run(allocator)
}
