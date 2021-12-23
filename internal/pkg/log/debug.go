// nolint:forbidigo // allow usage of the "zap" package
package log

import (
	"io"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/ioutil"
)

// debugLogger implements DebugLogger interface.
// Logs are stored in a buffer by ioutil.Writer.
type debugLogger struct {
	*zapLogger
	all         *ioutil.Writer
	debug       *ioutil.Writer
	info        *ioutil.Writer
	warn        *ioutil.Writer
	warnOrError *ioutil.Writer
	error       *ioutil.Writer
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
	l := &debugLogger{
		all:         ioutil.NewBufferedWriter(),
		debug:       ioutil.NewBufferedWriter(),
		info:        ioutil.NewBufferedWriter(),
		warn:        ioutil.NewBufferedWriter(),
		warnOrError: ioutil.NewBufferedWriter(),
		error:       ioutil.NewBufferedWriter(),
	}
	l.zapLogger = loggerFromZap(zap.New(zapcore.NewTee(
		debugCore(l.all, DebugLevel),                            // all = debug level and higher
		debugCore(l.debug, &oneLevelEnabler{level: DebugLevel}), // only debug msgs
		debugCore(l.info, &oneLevelEnabler{level: InfoLevel}),   // only info msgs
		debugCore(l.warn, &oneLevelEnabler{level: WarnLevel}),   // only warn msgs
		debugCore(l.warnOrError, WarnLevel),                     // warn or error = warn level and higher
		debugCore(l.error, ErrorLevel),                          // error = error level and higher
	)))
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

// AllMsgs returns all messages and Truncate all messages.
func (l *debugLogger) AllMsgs() string {
	return l.all.String()
}

// DebugMsgs returns all debug messages and Truncate all messages.
func (l *debugLogger) DebugMsgs() string {
	defer l.Truncate()
	return l.debug.String()
}

// InfoMsgs returns all info messages and Truncate all messages.
func (l *debugLogger) InfoMsgs() string {
	defer l.Truncate()
	return l.info.String()
}

// WarnMsgs returns all warn messages and Truncate all messages.
func (l *debugLogger) WarnMsgs() string {
	defer l.Truncate()
	return l.warn.String()
}

// WarnOrErrorMsgs returns all warn or error messages and Truncate all messages.
func (l *debugLogger) WarnOrErrorMsgs() string {
	defer l.Truncate()
	return l.warnOrError.String()
}

// ErrorMsgs returns all error messages and Truncate all messages.
func (l *debugLogger) ErrorMsgs() string {
	defer l.Truncate()
	return l.error.String()
}

func (l *debugLogger) allWriters() []*ioutil.Writer {
	return []*ioutil.Writer{l.all, l.debug, l.info, l.warn, l.warnOrError, l.error}
}

func debugCore(writer *ioutil.Writer, level zapcore.LevelEnabler) zapcore.Core {
	encoderConfig := zapcore.EncoderConfig{
		TimeKey:          "ts",
		LevelKey:         "level",
		MessageKey:       "msg",
		EncodeLevel:      zapcore.CapitalLevelEncoder,
		ConsoleSeparator: "  ",
	}
	return zapcore.NewCore(
		zapcore.NewConsoleEncoder(encoderConfig),
		zapcore.AddSync(writer),
		level,
	)
}
