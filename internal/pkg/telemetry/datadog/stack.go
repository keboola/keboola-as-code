package datadog

import (
	"runtime"
	"strconv"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type stackTracer interface {
	StackTrace() errors.StackTrace
}

// takeStacktrace from dd-trace-go library.
func takeStacktrace(n int, skip uint) string {
	pcs := make([]uintptr, n)
	numFrames := runtime.Callers(2+int(skip), pcs)
	if numFrames == 0 {
		return ""
	}
	return formatStackTrace(pcs, numFrames)
}

// formatStackTrace from dd-trace-go library.
func formatStackTrace(pcs []uintptr, numFrames int) string {
	var builder strings.Builder
	frames := runtime.CallersFrames(pcs[:numFrames])
	for i := 0; ; i++ {
		frame, more := frames.Next()
		if i != 0 {
			builder.WriteByte('\n')
		}
		builder.WriteString(frame.Function)
		builder.WriteByte('\n')
		builder.WriteByte('\t')
		builder.WriteString(frame.File)
		builder.WriteByte(':')
		builder.WriteString(strconv.Itoa(frame.Line))
		if !more {
			break
		}
	}
	return builder.String()
}
