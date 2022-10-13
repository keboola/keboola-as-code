package errors

import (
	"fmt"
	"runtime"
	"strings"
)

// Formatter is a configurable error formatter.
type Formatter interface {
	// WithMessageFormatter returns clone formatter with a custom MessageFormatter.
	WithMessageFormatter(MessageFormatter) Formatter
	// WithPrefixFormatter returns clone formatter with a custom PrefixFormatter.
	WithPrefixFormatter(PrefixFormatter) Formatter
	// Format error to string.
	Format(err error) string
	// FormatWithDebug output includes also errors stack traces if present.
	FormatWithDebug(err error) string
}

type formatter struct {
	messageFormatter MessageFormatter
	prefixFormatter  PrefixFormatter
}

// MessageFormatter formats each error message. StackTrace is present in the debug mode, see defaultMessageFormatter.
type MessageFormatter func(msg string, trace StackTrace) string

// PrefixFormatter formats a prefix followed by a list of errors, see defaultPrefixFormatter.
type PrefixFormatter func(prefix string) string

// defaultFormatter to save memory allocations.
var defaultFormatter = NewFormatter() // nolint: gochecknoglobals

func NewFormatter() Formatter {
	return &formatter{messageFormatter: defaultMessageFormatter(), prefixFormatter: defaultPrefixFormatter()}
}

func Format(err error) string {
	return defaultFormatter.Format(err)
}

func FormatWithDebug(err error) string {
	return defaultFormatter.FormatWithDebug(err)
}

func defaultMessageFormatter() MessageFormatter {
	return func(msg string, trace StackTrace) string {
		if len(trace) > 0 {
			frame := trace[0]
			fn := runtime.FuncForPC(frame)
			file, line := fn.FileLine(frame)
			msg = fmt.Sprintf("%s [%s:%d]", msg, file, line)
		}
		return msg
	}
}

func defaultPrefixFormatter() PrefixFormatter {
	return func(prefix string) string {
		return strings.TrimRight(prefix, ".,:") + ":"
	}
}

func (f *formatter) WithMessageFormatter(fn MessageFormatter) Formatter {
	clone := *f
	clone.messageFormatter = fn
	return &clone
}

func (f *formatter) WithPrefixFormatter(fn PrefixFormatter) Formatter {
	clone := *f
	clone.prefixFormatter = fn
	return &clone
}

func (f *formatter) Format(err error) string {
	w := NewWriter(f.messageFormatter, f.prefixFormatter)
	w.WriteError(err)
	return w.String()
}

func (f *formatter) FormatWithDebug(err error) string {
	w := NewWriter(f.messageFormatter, f.prefixFormatter).WithDebugOutput()
	w.WriteError(err)
	return w.String()
}
