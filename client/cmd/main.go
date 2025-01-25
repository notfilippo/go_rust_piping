package main

import (
	"github.com/notfilippo/go_rust_piping/client"
	"github.com/notfilippo/go_rust_piping/client/mem"
)

func main() {
	client.Run(mem.CGoAllocator)
}
