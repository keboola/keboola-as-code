package errors

import (
	"bufio"
	"fmt"
	"strings"
)

const (
	Indent = "  "
	Bullet = "- "
)

type Writer interface {
	Write(string)
	WriteIndent(level int)
	WriteBullet()
	WritePrefix(prefix string, trace StackTrace)
	WriteMessage(msg string, trace StackTrace)
	WriteNewLine()
	WriteError(err error)
	WriteErrorLevel(level int, err error, trace StackTrace)
	WriteNestedError(level int, main error, errs []error, trace StackTrace)
	WriteErrorsList(level int, errs []error)
	String() string
}

type errorWithWrite interface {
	WriteError(w Writer, level int, trace StackTrace)
}

type writer struct {
	config           FormatConfig
	out              strings.Builder
	messageFormatter MessageFormatter
	prefixFormatter  PrefixFormatter
}

func NewWriter(messageFormatter MessageFormatter, prefixFormatter PrefixFormatter, opts ...FormatOption) Writer {
	// Apply options
	config := FormatConfig{}
	for _, o := range opts {
		o(&config)
	}
	return &writer{config: config, messageFormatter: messageFormatter, prefixFormatter: prefixFormatter}
}

func (w *writer) WriteError(err error) {
	w.WriteErrorLevel(0, err, nil)
}

func (w *writer) WriteErrorLevel(level int, err error, trace StackTrace) {
	if err == nil {
		panic(Errorf("error cannot be nil"))
	}

	// Get trace, if it is present
	if v, ok := err.(stackTracer); ok { // nolint: errorlint
		trace = v.StackTrace()
	}

	// Else use parent trace (trace parameter) or trace from a sub error
	if trace == nil {
		var tracer stackTracer
		if As(err, &tracer) {
			trace = tracer.StackTrace()
		}
	}

	// nolint:errorlint
	switch v := err.(type) {
	case nestedErrorGetter:
		w.WriteNestedError(level, v.MainError(), v.WrappedErrors(), trace)
		return
	case multiErrorGetter:
		w.WriteErrorsList(level, v.WrappedErrors())
		return
	case *withStack:
		w.WriteErrorLevel(level, v.Unwrap(), trace)
		return
	case errorWithWrite:
		v.WriteError(w, level, trace)
		return
	default:
		if w.config.WithUnwrap {
			if subErr := Unwrap(v); subErr != nil {
				// Write current error
				w.Write(w.formatPrefix(w.formatMessage(v.Error(), trace)))
				w.WriteNewLine()

				// Write wrapped error
				w.WriteIndent(level)
				w.WriteBullet()
				w.Write(fmt.Sprintf("%T >>> ", err))
				w.WriteErrorLevel(level+1, subErr, nil)
				return
			}
		}

		// If the error contains more lines (which shouldn't happen), align all lines, see test.
		scanner := bufio.NewScanner(strings.NewReader(w.formatMessage(v.Error(), trace)))
		scanner.Scan()
		w.Write(scanner.Text())
		for scanner.Scan() {
			w.WriteNewLine()
			w.WriteIndent(level)
			w.Write(scanner.Text())
		}
	}
}

func (w *writer) WriteNestedError(level int, main error, errs []error, trace StackTrace) {
	// Convert main error to string
	mainWriter := w.clone()
	mainWriter.WriteErrorLevel(level, main, trace)
	mainStr := mainWriter.String()

	// Check if there is a sub error
	errsCount := len(errs)
	if errsCount == 0 {
		w.Write(mainStr)
		return
	}

	// Convert main error to prefix
	mainStr = w.formatPrefix(mainStr)

	// Convert sub errors to string
	subErrsWriter := w.clone()
	subErrsWriter.WriteErrorsList(level, errs)
	subErrsStr := subErrsWriter.String()

	// If there is more than one error or the message is long,
	// then break line and create bullet list
	w.Write(mainStr)
	if errsCount > 1 || len(mainStr)+len(subErrsStr) > 60 || strings.Contains(subErrsStr, "\n") {
		w.WriteNewLine()
		if errsCount == 1 {
			w.WriteIndent(level)
			w.WriteBullet()
			w.WriteErrorLevel(level+1, errs[0], nil)
		} else {
			w.WriteErrorsList(level, errs)
		}
	} else {
		w.Write(" ")
		w.WriteErrorsList(level, errs)
	}
}

func (w *writer) WriteErrorsList(level int, errs []error) {
	indent := len(errs) > 1
	last := len(errs) - 1
	for i, err := range errs {
		if indent {
			w.WriteIndent(level)
			w.WriteBullet()
		}
		w.WriteErrorLevel(level+1, err, nil)
		if i != last {
			w.WriteNewLine()
		}
	}
}

func (w *writer) WriteIndent(level int) {
	w.Write(strings.Repeat(Indent, level))
}

func (w *writer) WriteBullet() {
	w.Write(Bullet)
}

func (w *writer) WriteNewLine() {
	w.Write("\n")
}

func (w *writer) Write(s string) {
	_, _ = w.out.WriteString(s)
}

func (w *writer) WritePrefix(prefix string, trace StackTrace) {
	w.Write(w.formatPrefix(w.formatMessage(prefix, trace)))
}

func (w *writer) WriteMessage(msg string, trace StackTrace) {
	w.Write(w.formatMessage(msg, trace))
}

func (w *writer) String() string {
	return w.out.String()
}

func (w *writer) clone() Writer {
	clone := *w
	clone.out.Reset()
	return &clone
}

func (w *writer) formatMessage(message string, trace StackTrace) string {
	return w.messageFormatter(message, trace, w.config)
}

func (w *writer) formatPrefix(prefix string) string {
	return w.prefixFormatter(prefix)
}
