package api

import (
	"fmt"
	"go.uber.org/zap"
	"regexp"
)

const ClientLoggerPrefix = "HTTP%s\t"

type ClientLogger struct {
	logger *zap.SugaredLogger
}

func (l *ClientLogger) Debugf(format string, v ...interface{}) {
	l.logWithoutSecrets("", format, v...)
}

func (l *ClientLogger) Warnf(format string, v ...interface{}) {
	l.logWithoutSecrets("-WARN", format, v...)
}

func (l *ClientLogger) Errorf(format string, v ...interface{}) {
	l.logWithoutSecrets("-ERROR", format, v...)
}

func (l *ClientLogger) logWithoutSecrets(level string, format string, v ...interface{}) {
	v = append([]interface{}{level}, v...)
	msg := fmt.Sprintf(ClientLoggerPrefix+format, v...)
	msg = regexp.MustCompile(`(?i)(token:?\s*)[^\s]+`).ReplaceAllString(msg, "$1*****")
	l.logger.Debug(msg)
}
