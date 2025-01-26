package zap

// #cgo CFLAGS: -I../../zap/include
// #cgo linux LDFLAGS: -L../../target/debug -lzap -lm
// #cgo darwin LDFLAGS: -L../../target/debug -lzap -lm -framework CoreFoundation -framework CoreServices -framework Security -framework SystemConfiguration
// #include <zap.h>
import "C"

import (
	"os"
	"syscall"
	"unsafe"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/cdata"
)

type inputMessage = C.InputMessage
type outputMessage = C.OutputMessage

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
