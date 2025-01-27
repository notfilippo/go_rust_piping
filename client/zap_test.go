package client_test

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/notfilippo/go_rust_piping/client/mem"
	"github.com/notfilippo/go_rust_piping/client/testdata"
	"github.com/notfilippo/go_rust_piping/client/zap"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestExecuteStream(t *testing.T) {
	for _, query := range testdata.Queries {
		t.Run(query.Name, func(t *testing.T) {
			allocator := mem.NewLeakCheckAllocator(mem.CGoAllocator)
			defer mem.CheckAllocatorLeaks(t, allocator, false)
			executeStream(allocator, query)
		})
	}
}

func executeStream(allocator memory.Allocator, query testdata.Query) {
	executor := zap.Start()
	defer executor.Close()

	stream, err := executor.Query(query.Sql, testdata.Schema)
	if err != nil {
		panic(err)
	}

	var group errgroup.Group
	haveResult := make(chan struct{})

	group.Go(func() error {
		for i := range testdata.DefaultRecordCount {
			select {
			case <-haveResult:
				return stream.CloseSend()
			default:
			}

			record := testdata.NewRecord(i, allocator)

			err = stream.Write(record)
			record.Release()
			if err != nil {
				if err == io.EOF {
					return stream.CloseSend()
				}
				return errors.Join(err, stream.CloseSend())
			}
		}

		return stream.CloseSend()
	})

	group.Go(func() error {
		defer close(haveResult)
		for {
			record, err := stream.Read()
			if err != nil {
				if err == io.EOF {
					return nil
				}

				return err
			}
			record.Release()
		}
	})

	if err := group.Wait(); err != nil {
		panic(err)
	}
}

func TestExecuteChannel(t *testing.T) {
	for _, query := range testdata.Queries {
		t.Run(query.Name, func(t *testing.T) {
			allocator := mem.NewLeakCheckAllocator(mem.CGoAllocator)
			defer mem.CheckAllocatorLeaks(t, allocator, false)
			executeChannel(allocator, query)
		})
	}
}

func executeChannel(allocator memory.Allocator, query testdata.Query) {
	executor := zap.Start()
	defer executor.Close()

	stream, err := executor.Query(query.Sql, testdata.Schema)
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	input := make(chan arrow.Record)
	output := stream.Channelize(ctx, input)

	go func() {
		defer close(input)
		for i := range testdata.DefaultRecordCount {
			record := testdata.NewRecord(i, allocator)
			select {
			case <-ctx.Done():
				record.Release()
				return
			case input <- record:
			}
		}
	}()

	for record := range output {
		record.Release()
	}
}

func TestReachChannelLimit(t *testing.T) {
	allocator := mem.NewLeakCheckAllocator(mem.CGoAllocator)
	defer mem.CheckAllocatorLeaks(t, allocator, false)

	executor := zap.Start()
	defer executor.Close()

	stream, err := executor.Query("SELECT integers + 1 FROM data", testdata.Schema)
	if err != nil {
		panic(err)
	}

	record := testdata.NewRecord(0, allocator)
	defer record.Release()

	var (
		stopSending atomic.Bool
		lastSent    atomic.Int64
	)

	lastSent.Store(time.Now().Unix())

	var sent, received int

	go func() {
		for !stopSending.Load() {
			if err := stream.Write(record); err != nil {
				panic(err)
			}
			lastSent.Store(time.Now().Unix())
			sent++
		}

		err := stream.CloseSend()
		if err != nil {
			panic(err)
		}

	}()

	for range time.Tick(time.Second) {
		// If no record was sent in the last 5 seconds, stop sending
		if time.Since(time.Unix(lastSent.Load(), 0)) > 5*time.Second {
			stopSending.Store(true)
			break
		}
	}

	// Read all records
	for {
		record, err := stream.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		record.Release()
		received++
	}

	require.Equal(t, sent, received, "sent and received records should be equal")
}

func BenchmarkStream(b *testing.B) {
	for _, query := range testdata.Queries {
		b.Run(query.Name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				executeStream(mem.CGoAllocator, query)
			}
		})
	}
}

func BenchmarkStreamParallel(b *testing.B) {
	for _, query := range testdata.Queries {
		b.Run(query.Name+"Parallel", func(b *testing.B) {
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					executeStream(mem.CGoAllocator, query)
				}
			})
		})
	}
}

func BenchmarkChannel(b *testing.B) {
	for _, query := range testdata.Queries {
		b.Run(query.Name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				executeChannel(mem.CGoAllocator, query)
			}
		})
	}
}
func BenchmarkChannelParallel(b *testing.B) {
	for _, query := range testdata.Queries {
		b.Run(query.Name+"Parallel", func(b *testing.B) {
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					executeChannel(mem.CGoAllocator, query)
				}
			})
		})
	}
}
