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

const (
	DefaultRecordCount = 100
	DefaultRecordSize  = 100
)

var (
	Schema = arrow.NewSchema(
		[]arrow.Field{
			{Name: "integers", Type: arrow.PrimitiveTypes.Int32},
			{Name: "floats", Type: arrow.PrimitiveTypes.Float64},
		},
		nil,
	)
)

type Query struct {
	Name        string
	Description string
	Sql         string
	RecordCount int // if <= 0, defaults to DefaultRecordCount
}

var queries = []Query{
	{
		Name:        "SelectLimit10",
		Description: "Select the first 10 rows from the table",
		Sql:         `SELECT * FROM data LIMIT 10;`,
	},
	{
		Name:        "FilterIntegersGT10",
		Description: "Select rows where integers are greater than 10, limit to 20 rows",
		Sql:         `SELECT * FROM data WHERE integers > 10 LIMIT 20;`,
	},
	{
		Name:        "FilterFloatsBetween1And5",
		Description: "Select rows where floats are between 1.0 and 5.0, limit to 15 rows",
		Sql:         `SELECT * FROM data WHERE floats BETWEEN 1.0 AND 5.0 LIMIT 15;`,
	},
	{
		Name:        "OrderByIntegersDesc",
		Description: "Order rows by integers in descending order, limit to 25 rows",
		Sql:         `SELECT * FROM data ORDER BY integers DESC LIMIT 25;`,
	},
	{
		Name:        "OrderByFloatsAsc",
		Description: "Order rows by floats in ascending order, limit to 20 rows",
		Sql:         `SELECT * FROM data ORDER BY floats ASC LIMIT 20;`,
	},
	{
		Name:        "MultiplyIntegersBy3",
		Description: "Select integers and their multiplication by 3, limit to 15 rows",
		Sql:         `SELECT integers, integers * 3 AS tripled FROM data LIMIT 15;`,
	},
	{
		Name:        "AddIntegersAndFloats",
		Description: "Select integers, floats, and their sum, limit to 10 rows",
		Sql:         `SELECT integers, floats, integers + floats AS sum FROM data LIMIT 10;`,
	},
	{
		Name:        "GroupByIntegersCount",
		Description: "Group by integers and count rows per group, limit to 10 groups",
		Sql: `SELECT integers, COUNT(*) AS count_per_integer
										FROM data
										GROUP BY integers
										ORDER BY count_per_integer DESC LIMIT 10;`,
	},
	{
		Name:        "MinIntegers",
		Description: "Select the minimum integer value in the table",
		Sql:         `SELECT MIN(integers) AS min_integer FROM data;`,
	},
	{
		Name:        "SumIntegers",
		Description: "Calculate the sum of integers in the table",
		Sql:         `SELECT SUM(integers) AS sum_integers FROM data;`,
	},
	{
		Name:        "CountRows",
		Description: "Count the total number of rows in the table",
		Sql:         `SELECT COUNT(*) AS total_rows FROM data;`,
	},
	{
		Name:        "ParityGroupCount",
		Description: "Group rows by parity (even or odd integers) and count rows per group",
		Sql: `SELECT CASE WHEN integers % 2 = 0 THEN 'even' ELSE 'odd' END AS parity, COUNT(*) AS count
										FROM data
										GROUP BY parity
										ORDER BY count DESC LIMIT 2;`,
	},
}

func newRecord(index int, allocator memory.Allocator) arrow.Record {
	b := array.NewRecordBuilder(allocator, Schema)
	defer b.Release()

	const recordLength = DefaultRecordSize

	var (
		ints   []int32
		floats []float64
	)
	for range recordLength {
		ints = append(ints, int32(index))
		floats = append(floats, float64(index)+0.5)
	}

	b.Field(0).(*array.Int32Builder).Reserve(recordLength)
	b.Field(0).(*array.Int32Builder).AppendValues(ints, nil)

	b.Field(1).(*array.Float64Builder).Reserve(recordLength)
	b.Field(1).(*array.Float64Builder).AppendValues(floats, nil)

	return b.NewRecord()
}

func TestExecuteStream(t *testing.T) {
	for _, query := range queries {
		if query.RecordCount <= 0 {
			query.RecordCount = DefaultRecordCount
		}

		t.Run(query.Name, func(t *testing.T) {
			allocator := mem.NewLeakCheckAllocator(mem.CGoAllocator)
			defer mem.CheckAllocatorLeaks(t, allocator, false)
			executeStream(allocator, query)
		})
	}
}

func executeStream(allocator memory.Allocator, query Query) {
	executor := zap.Start()
	defer executor.Close()

	stream, err := executor.Query(query.Sql, Schema)
	if err != nil {
		panic(err)
	}

	var group errgroup.Group
	haveResult := make(chan struct{})

	group.Go(func() error {
		for i := range query.RecordCount {
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
	for _, query := range queries {
		if query.RecordCount <= 0 {
			query.RecordCount = DefaultRecordCount
		}

		t.Run(query.Name, func(t *testing.T) {
			allocator := mem.NewLeakCheckAllocator(mem.CGoAllocator)
			defer mem.CheckAllocatorLeaks(t, allocator, false)
			executeChannel(allocator, query)
		})
	}
}

func executeChannel(allocator memory.Allocator, query Query) {
	executor := zap.Start()
	defer executor.Close()

	stream, err := executor.Query(query.Sql, Schema)
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	input := make(chan arrow.Record)
	output := stream.Channelize(ctx, input)

	go func() {
		defer close(input)
		for i := range query.RecordCount {
			record := newRecord(i, allocator)
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

	stream, err := executor.Query("SELECT integers + 1 FROM data", Schema)
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

func BenchmarkStream(b *testing.B) {
	for _, query := range queries {
		if query.RecordCount <= 0 {
			query.RecordCount = DefaultRecordCount
		}

		b.Run(query.Name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				executeStream(mem.CGoAllocator, query)
			}
		})
	}
}

func BenchmarkChannel(b *testing.B) {
	for _, query := range queries {
		if query.RecordCount <= 0 {
			query.RecordCount = DefaultRecordCount
		}

		b.Run(query.Name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				executeChannel(mem.CGoAllocator, query)
			}
		})
	}
}
