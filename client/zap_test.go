package client_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"golang.org/x/sync/errgroup"

	"github.com/notfilippo/go_rust_piping/client/mem"
	"github.com/notfilippo/go_rust_piping/client/zap"
)

var (
	schema = arrow.NewSchema(
		[]arrow.Field{
			{Name: "integers", Type: arrow.PrimitiveTypes.Int32},
			{Name: "floats", Type: arrow.PrimitiveTypes.Float64},
		},
		nil,
	)
)

func newRecord(index int, allocator memory.Allocator) arrow.Record {
	b := array.NewRecordBuilder(allocator, schema)
	defer b.Release()

	const recordLength = 10

	var (
		ints   []int32
		floats []float64
	)
	for range recordLength {
		ints = append(ints, int32(index))
		floats = append(floats, float64(index)+0.5)
	}

	b.Field(0).(*array.Int32Builder).Reserve(1)
	b.Field(0).(*array.Int32Builder).AppendValues(ints, nil)
	b.Field(1).(*array.Float64Builder).Reserve(1)
	b.Field(1).(*array.Float64Builder).AppendValues(floats, nil)

	return b.NewRecord()
}

func TestClientStream(t *testing.T) {
	allocator := mem.NewLeakCheckAllocator(mem.CGoAllocator)
	defer mem.CheckAllocatorLeaks(t, allocator, false)

	executor := zap.Start()
	defer executor.Close()

	stream, err := executor.Query("SELECT * FROM data WHERE integers = 2 LIMIT 1", schema)
	if err != nil {
		panic(err)
	}

	var group errgroup.Group
	haveResult := make(chan struct{})

	group.Go(func() error {
		fmt.Println("client: writing records")

		i := 0
		for {
			select {
			case <-haveResult:
				return stream.CloseSend()
			default:
			}

			record := newRecord(i, allocator)

			err = stream.Write(record)
			record.Release()
			if err != nil {
				if err == io.EOF {
					return stream.CloseSend()
				}
				return errors.Join(err, stream.CloseSend())
			}

			fmt.Println("client: sent record", i)
			i += 1
		}
	})

	group.Go(func() error {
		defer close(haveResult)
		fmt.Println("client: reading records")
		index := 0
		for {
			record, err := stream.Read()
			if err != nil {
				if err == io.EOF {
					return nil
				}

				return err
			}

			fmt.Println("client: received record", index, record)
			index++
			record.Release()
		}
	})

	if err := group.Wait(); err != nil {
		panic(err)
	}
}

func TestClientChannel(t *testing.T) {
	allocator := mem.NewLeakCheckAllocator(mem.CGoAllocator)
	defer mem.CheckAllocatorLeaks(t, allocator, false)

	executor := zap.Start()
	defer executor.Close()

	stream, err := executor.Query("SELECT * FROM data WHERE integers = 2 LIMIT 1", schema)
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	input := make(chan arrow.Record)
	output := stream.Channelize(ctx, input)

	go func() {
		defer close(input)
		i := 0
		for {
			record := newRecord(i, allocator)
			select {
			case <-ctx.Done():
				record.Release()
				return
			case input <- record:
			}
			fmt.Println("client: sent record", i)
			i++
		}
	}()

	for record := range output {
		fmt.Println("client: received record", record)
		record.Release()
	}
}

func TestReachChannelLimit(t *testing.T) {
	t.Skip()

	allocator := mem.NewLeakCheckAllocator(mem.CGoAllocator)
	defer mem.CheckAllocatorLeaks(t, allocator, false)

	executor := zap.Start()
	defer executor.Close()

	stream, err := executor.Query("SELECT integers + 1 FROM data", schema)
	if err != nil {
		panic(err)
	}

	record := newRecord(0, allocator)
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

	fmt.Println("client: received", received, "records")
	fmt.Println("client: sent", sent, "records")
}
