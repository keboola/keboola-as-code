package client

import (
	"fmt"
	"regexp"
	"strings"

	"go.uber.org/zap"
)

const LoggerPrefix = "HTTP%s\t"

// Logger for HTTP client.
type Logger struct {
	logger *zap.SugaredLogger
}

func (l *Logger) Debugf(format string, v ...interface{}) {
	l.logWithoutSecrets("", format, v...)
}

func (l *Logger) Warnf(format string, v ...interface{}) {
	l.logWithoutSecrets("-WARN", format, v...)
}

func (l *Logger) Errorf(format string, v ...interface{}) {
	l.logWithoutSecrets("-ERROR", format, v...)
}

func (l *Logger) logWithoutSecrets(level string, format string, v ...interface{}) {
	v = append([]interface{}{level}, v...)
	msg := fmt.Sprintf(LoggerPrefix+format, v...)
	msg = removeSecrets(msg)
	msg = strings.TrimSuffix(msg, "\n")
	l.logger.Debug(msg)
}

func removeSecrets(str string) string {
	return regexp.MustCompile(`(?i)(token[^\w/,]\s*)\d[^\s/]*`).ReplaceAllString(str, "$1*****")
}
