package log

import (
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type LogFormat string

const (
	LogFormatConsole LogFormat = "console"
	LogFormatJSON    LogFormat = "json"
)

// Creates LogFormat from string.
// On invalid value Console is used as default with an error.
func NewLogFormat(format string) (LogFormat, error) {
	logFormat := LogFormat(format)

	switch logFormat {
	case LogFormatConsole, LogFormatJSON:
		return logFormat, nil
	default:
		return LogFormatConsole, errors.New(`log format must be "console" or "json"`)
	}
}
