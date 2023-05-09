package oteldd

import (
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

type ddLogger struct {
	log.Logger
}

func (l ddLogger) Log(msg string) {
	l.Logger.AddPrefix("[datadog]").Info(msg)
}

func NewDDLogger(logger log.Logger) ddtrace.Logger {
	return &ddLogger{Logger: logger}
}
