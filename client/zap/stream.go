package zap

import (
	"io"
	"os"
	"unsafe"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/cdata"
)

type Stream struct {
	inputProducer  *os.File
	outputConsumer *os.File
}

const sizeOfInput = unsafe.Sizeof(inputMessage{})
const sizeOfOutput = unsafe.Sizeof(outputMessage{})

func inputMessageBytes(msg *inputMessage) []byte {
	return (*[sizeOfInput]byte)(unsafe.Pointer(msg))[:]
}

func outputMessageBytes(msg *outputMessage) []byte {
	return (*[sizeOfOutput]byte)(unsafe.Pointer(msg))[:]
}

func (s *Stream) Write(record arrow.Record) error {
	var cArray cdata.CArrowArray
	cdata.ExportArrowRecordBatch(record, &cArray, nil)

	var msg inputMessage
	array := (*cdata.CArrowArray)(unsafe.Pointer(&msg.array))
	*array = cArray

	buf := inputMessageBytes(&msg)

	// Write all the bytes in the buffer
	for len(buf) > 0 {
		n, err := s.inputProducer.Write(buf)
		if err != nil {
			cdata.ReleaseCArrowArray(&cArray)
			return err
		}

		if n == 0 {
			cdata.ReleaseCArrowArray(&cArray)
			return io.EOF
		} else {
			buf = buf[n:]
		}
	}

	return nil
}

func (s *Stream) Read() (arrow.Record, error) {
	var msg outputMessage
	buf := outputMessageBytes(&msg)

	// Read exactly the number of bytes in the buffer
	for len(buf) > 0 {
		n, err := s.outputConsumer.Read(buf)
		if err != nil {
			return nil, err
		}

		if n == 0 {
			return nil, io.EOF
		} else {
			buf = buf[n:]
		}
	}

	array := (*cdata.CArrowArray)(unsafe.Pointer(&msg.array))
	schema := (*cdata.CArrowSchema)(unsafe.Pointer(&msg.schema))

	defer cdata.ReleaseCArrowArray(array)
	defer cdata.ReleaseCArrowSchema(schema)

	return cdata.ImportCRecordBatch(array, schema)
}

func (s *Stream) CloseSend() error {
	return s.inputProducer.Close()
}
