package main

import (
	"fmt"
	"io"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/notfilippo/go_rust_piping/client/allocator"
	"github.com/notfilippo/go_rust_piping/client/zap"
	"golang.org/x/sync/errgroup"
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

func NewRecord(index int, allocator memory.Allocator) arrow.Record {
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

func main() {
	executor := zap.Start()
	defer executor.Close()

	stream, err := executor.Query("SELECT * FROM data LIMIT 1", schema)
	if err != nil {
		panic(err)
	}

	var group errgroup.Group
	haveResult := make(chan struct{})

	group.Go(func() error {
		fmt.Println("client: writing records")

		for i := 0; i < 10; i++ {
			select {
			case <-haveResult:
				return stream.CloseSend()
			default:
			}

			record := NewRecord(i, allocator.CGoAllocator)
			stream.Write(record)
			record.Release()
		}

		return stream.CloseSend()
	})

	group.Go(func() error {
		defer close(haveResult)
		fmt.Println("client: reading records")
		for {
			record, err := stream.Read()
			if err != nil {
				if err == io.EOF {
					return nil
				}

				return err
			}

			fmt.Println("client: received record", record)
			record.Release()
		}
	})

	if err := group.Wait(); err != nil {
		panic(err)
	}
}
