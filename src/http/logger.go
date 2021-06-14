package http

import (
	"fmt"
	"go.uber.org/zap"
	"regexp"
	"strings"
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
	msg = removeSecrets(msg)
	msg = strings.TrimSuffix(msg, "\n")
	l.logger.Debug(msg)
}

func removeSecrets(str string) string {
	return regexp.MustCompile(`(?i)(token[^\w/,]\s*)[^\s/]+`).ReplaceAllString(str, "$1*****")
}
