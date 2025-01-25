package zap

import (
	"context"
	"github.com/apache/arrow/go/v17/arrow"
	"io"
)

func (s *Stream) Channelize(ctx context.Context, input <-chan arrow.Record) <-chan arrow.Record {
	output := make(chan arrow.Record)

	go inputLoop(ctx, s, input)
	go outputLoop(ctx, s, output)

	return output
}

func inputLoop(ctx context.Context, stream *Stream, input <-chan arrow.Record) {
	defer func() { _ = stream.CloseSend() }()

	for {
		select {
		case <-ctx.Done():
			return
		case record := <-input:
			if record == nil {
				return
			}

			err := stream.Write(record)
			record.Release()

			if err != nil {
				if err == io.EOF {
					return
				}

				panic(err)
			}
		}
	}
}

func outputLoop(ctx context.Context, stream *Stream, output chan<- arrow.Record) {
	defer close(output)

	for {
		record, err := stream.Read()
		if err != nil {
			if err == io.EOF {
				return
			}

			panic(err)
		}

		select {
		case <-ctx.Done():
			return
		case output <- record:
		}
	}
}
