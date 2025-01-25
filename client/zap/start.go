package zap

// #cgo CFLAGS: -I../../zap/include
// #cgo LDFLAGS: -L../../target/debug -lzap -lm
// #include <zap.h>
import "C"

import (
	"os"
	"syscall"
	"unsafe"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/cdata"
)

type Executor struct {
	inner unsafe.Pointer
}

func Start() *Executor {
	executor := C.zap_start()
	return &Executor{executor}
}

func (e *Executor) Query(sql string, schema *arrow.Schema) (*Stream, error) {
	fds := make([]C.int, 2)
	errno := C.zap_pipe(&fds[0])
	if errno != 0 {
		return nil, syscall.Errno(errno)
	}

	inputProducer := os.NewFile(uintptr(fds[1]), "inputProducer")

	cSql := C.CString(sql)
	defer C.free(unsafe.Pointer(cSql))

	cSchema := new(cdata.CArrowSchema)
	cdata.ExportArrowSchema(schema, cSchema)
	defer cdata.ReleaseCArrowSchema(cSchema)

	outputReceiver := C.zap_query(e.inner, cSql, unsafe.Pointer(cSchema), C.int(fds[0]))

	outputConsumer := os.NewFile(uintptr(outputReceiver), "outputConsumer")

	return &Stream{inputProducer, outputConsumer, nil}, nil
}

func (e *Executor) Close() {
	C.zap_stop(e.inner)
}

type Stream struct {
	inputProducer  *os.File
	outputConsumer *os.File

	sent []*cdata.CArrowArray
}

const sizeOfInput = unsafe.Sizeof(C.InputMessage{})
const sizeOfOutput = unsafe.Sizeof(C.OutputMessage{})

func inputMessageBytes(msg *C.InputMessage) []byte {
	return (*[sizeOfInput]byte)(unsafe.Pointer(msg))[:]
}

func outputMessageBytes(msg *C.OutputMessage) []byte {
	return (*[sizeOfOutput]byte)(unsafe.Pointer(msg))[:]
}

func (s *Stream) Write(record arrow.Record) error {
	cArray := new(cdata.CArrowArray)
	cdata.ExportArrowRecordBatch(record, cArray, nil)
	s.sent = append(s.sent, cArray)

	var inputMessage C.InputMessage
	inputMessage.array = unsafe.Pointer(cArray)

	buf := inputMessageBytes(&inputMessage)
	n, err := s.inputProducer.Write(buf)
	if err != nil {
		return err
	}
	if n != len(buf) {
		return syscall.EIO
	}
	return nil
}

func (s *Stream) Read() (arrow.Record, error) {
	var outputMessage C.OutputMessage
	buf := outputMessageBytes(&outputMessage)
	n, err := s.outputConsumer.Read(buf)
	if err != nil {
		return nil, err
	}

	if n == 0 {
		return nil, nil
	} else if n != len(buf) {
		return nil, syscall.EIO
	}

	array := (*cdata.CArrowArray)(outputMessage.array)
	schema := (*cdata.CArrowSchema)(outputMessage.schema)

	defer cdata.ReleaseCArrowArray(array)
	defer cdata.ReleaseCArrowSchema(schema)

	return cdata.ImportCRecordBatch(array, schema)
}

func (s *Stream) CloseSend() error {
	return s.inputProducer.Close()
}
