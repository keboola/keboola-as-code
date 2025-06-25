package datadog

import (
	"context"

	"github.com/DataDog/dd-trace-go/v2/ddtrace/tracer"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

type ddLogger struct {
	log.Logger
}

func (l ddLogger) Log(msg string) {
	// DataDog library doesn't provide a context of the message, so we have no choice but to use context.Background().
	// It doesn't matter too much because it doesn't log anything most of the time or just incorrect configuration.
	l.Logger.WithComponent("datadog").Info(context.Background(), msg)
}

func NewDDLogger(logger log.Logger) tracer.Logger {
	return &ddLogger{Logger: logger}
}
