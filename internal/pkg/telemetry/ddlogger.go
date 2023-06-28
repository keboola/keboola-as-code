package telemetry

import (
	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace"
	"runtime/debug"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

type ddLogger struct {
	log.Logger
}

func (l ddLogger) Log(msg string) {
	l.Logger.AddPrefix("[datadog]").Info(msg + " " + json.MustEncodeString(string(debug.Stack()), false))
}

func NewDDLogger(logger log.Logger) ddtrace.Logger {
	return &ddLogger{Logger: logger}
}
