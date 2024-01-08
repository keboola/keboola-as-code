// nolint:forbidigo // allow usage of the "zap" package
package log

import (
	"bufio"
	"io"
	"strings"

	"go.uber.org/zap/zapcore"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/ioutil"
)

// debugLogger implements DebugLogger interface.
// Logs are stored in a buffer by ioutil.Writer.
type debugLogger struct {
	*zapLogger
	all         *ioutil.AtomicWriter
	debug       *ioutil.AtomicWriter
	info        *ioutil.AtomicWriter
	warn        *ioutil.AtomicWriter
	warnOrError *ioutil.AtomicWriter
	error       *ioutil.AtomicWriter
}

// oneLevelEnabler enables only one level. The others are discarded.
type oneLevelEnabler struct {
	level zapcore.Level
}

func (v *oneLevelEnabler) Enabled(level zapcore.Level) bool {
	return v.level == level
}

// NewDebugLogger returns logs as string by String() method.
// See also other methods of the ioutil.Writer.
func NewDebugLogger() DebugLogger {
	return NewDebugLoggerWithPrefix("")
}

func NewDebugLoggerWithPrefix(prefix string) DebugLogger {
	l := &debugLogger{
		all:         ioutil.NewAtomicWriter(),
		debug:       ioutil.NewAtomicWriter(),
		info:        ioutil.NewAtomicWriter(),
		warn:        ioutil.NewAtomicWriter(),
		warnOrError: ioutil.NewAtomicWriter(),
		error:       ioutil.NewAtomicWriter(),
	}
	cores := zapcore.NewTee(
		debugCore(l.all, DebugLevel),                            // all = debug level and higher
		debugCore(l.debug, &oneLevelEnabler{level: DebugLevel}), // only debug msgs
		debugCore(l.info, &oneLevelEnabler{level: InfoLevel}),   // only info msgs
		debugCore(l.warn, &oneLevelEnabler{level: WarnLevel}),   // only warn msgs
		debugCore(l.warnOrError, WarnLevel),                     // warn or error = warn level and higher
		debugCore(l.error, ErrorLevel),                          // error = error level and higher
	)
	if prefix != "" {
		cores = cores.With([]zapcore.Field{{Key: "prefix", String: prefix, Type: zapcore.StringType}})
	}
	l.zapLogger = loggerFromZapCore(cores)
	l.zapLogger.prefix = prefix
	return l
}

// ConnectTo connects all messages to a writer, for example os.Stdout.
func (l *debugLogger) ConnectTo(writer io.Writer) {
	l.all.ConnectTo(writer)
}

// Truncate clear all messages.
func (l *debugLogger) Truncate() {
	for _, w := range l.allWriters() {
		w.Truncate()
	}
}

// AllMessages returns all messages and Truncate all messages.
func (l *debugLogger) AllMessages() string {
	_ = l.Sync()
	return l.all.String()
}

// DebugMessages returns all debug messages and Truncate all messages.
func (l *debugLogger) DebugMessages() string {
	_ = l.Sync()
	return l.debug.String()
}

// InfoMessages returns all info messages and Truncate all messages.
func (l *debugLogger) InfoMessages() string {
	_ = l.Sync()
	return l.info.String()
}

// WarnMessages returns all warn messages and Truncate all messages.
func (l *debugLogger) WarnMessages() string {
	_ = l.Sync()
	return l.warn.String()
}

// WarnAndErrorMessages returns all warn or error messages and Truncate all messages.
func (l *debugLogger) WarnAndErrorMessages() string {
	_ = l.Sync()
	return l.warnOrError.String()
}

// ErrorMessages returns all error messages and Truncate all messages.
func (l *debugLogger) ErrorMessages() string {
	_ = l.Sync()
	return l.error.String()
}

// AllMessagesTxt returns all error messages as text only (without fields) and Truncate all messages.
// Panics on a non-json message.
func (l *debugLogger) AllMessagesTxt() string {
	_ = l.Sync()

	allMessages := l.all.String()
	scanner := bufio.NewScanner(strings.NewReader(strings.Trim(allMessages, "\n")))

	output := ""
	for scanner.Scan() {
		message := scanner.Text()
		var messageData map[string]any
		err := json.DecodeString(message, &messageData)
		if err != nil {
			panic(err)
		}

		message, ok := messageData["message"].(string)
		if !ok {
			panic(errors.New("log message is a json but does not have a \"message\" field"))
		}

		level, ok := messageData["level"].(string)
		if !ok {
			panic(errors.New("log message is a json but does not have a \"level\" field"))
		}

		output += strings.ToUpper(level) + "  " + message + "\n"
	}

	return output
}

func (l *debugLogger) allWriters() []*ioutil.AtomicWriter {
	return []*ioutil.AtomicWriter{l.all, l.debug, l.info, l.warn, l.warnOrError, l.error}
}

func debugCore(writer *ioutil.AtomicWriter, level zapcore.LevelEnabler) zapcore.Core {
	return zapcore.NewCore(
		newJSONEncoder(),
		writer,
		level,
	)
}
