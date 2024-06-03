package errors

import (
	"fmt"
	"runtime"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

// Formatter is a configurable error formatter.
type Formatter interface {
	// WithMessageFormatter returns clone formatter with a custom MessageFormatter.
	WithMessageFormatter(mf MessageFormatter) Formatter
	// WithPrefixFormatter returns clone formatter with a custom PrefixFormatter.
	WithPrefixFormatter(pf PrefixFormatter) Formatter
	// Format error to string.
	Format(err error, opts ...FormatOption) string
}

type formatter struct {
	options          []FormatOption
	messageFormatter MessageFormatter
	prefixFormatter  PrefixFormatter
}

// MessageFormatter formats each error message. StackTrace is present in the debug mode, see defaultMessageFormatter.
type MessageFormatter func(msg string, trace StackTrace, config FormatConfig) string

// PrefixFormatter formats a prefix followed by a list of errors, see defaultPrefixFormatter.
type PrefixFormatter func(prefix string) string

// defaultFormatter to save memory allocations.
var defaultFormatter = NewFormatter() // nolint: gochecknoglobals

func NewFormatter(opts ...FormatOption) Formatter {
	return &formatter{options: opts, messageFormatter: defaultMessageFormatter(), prefixFormatter: defaultPrefixFormatter()}
}

func Format(err error, opts ...FormatOption) string {
	return defaultFormatter.Format(err, opts...)
}

func defaultMessageFormatter() MessageFormatter {
	return func(msg string, trace StackTrace, config FormatConfig) string {
		// Uppercase first letter and add dot to the end, if message doesn't end with a special character
		if config.AsSentences {
			msg = strhelper.AsSentence(msg)
		}
		// Add last frame from the trace
		if config.WithStack && len(trace) > 0 {
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

func (f *formatter) Format(err error, opts ...FormatOption) string {
	w := NewWriter(f.messageFormatter, f.prefixFormatter, append(f.options, opts...)...)
	w.WriteError(err)
	return w.String()
}
