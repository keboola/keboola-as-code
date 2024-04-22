package log

import (
	"context"
	"log"
)

// forwardErrorWriter - see Write method and https://stackoverflow.com/a/52298092.
type forwardErrorWriter struct {
	logger Logger
}

// The standard log.Logger type guarantees that each log message
// is delivered to the destination io.Writer with a single Writer.Write() call.
// So we can safely forward the write call to our logger.
func (fw *forwardErrorWriter) Write(p []byte) (n int, err error) {
	fw.logger.Error(context.Background(), string(p))
	return len(p), nil
}

func NewStdErrorLogger(logger Logger) *log.Logger {
	return log.New(&forwardErrorWriter{logger: logger}, "", 0)
}
