package router

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
)

func TestSinkAcceptsSignal(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		allowed  []string
		signal   string
		expected bool
	}{
		// HTTP source: signal is always empty. The sink filter is ignored so the
		// documented "HTTP sources ignore allowedSignals" contract holds even
		// when an HTTP source reuses a sink that was configured for OTLP.
		{
			name:     "HTTP record, no filter",
			allowed:  nil,
			signal:   "",
			expected: true,
		},
		{
			name:     "HTTP record, filter set to logs only",
			allowed:  []string{definition.OTLPSignalLogs},
			signal:   "",
			expected: true,
		},
		{
			name:     "HTTP record, filter set to all OTLP signals",
			allowed:  []string{definition.OTLPSignalLogs, definition.OTLPSignalMetrics, definition.OTLPSignalTraces},
			signal:   "",
			expected: true,
		},

		// OTLP source: signal carries the OTLP signal type.
		{
			name:     "OTLP logs record, empty filter accepts all",
			allowed:  nil,
			signal:   definition.OTLPSignalLogs,
			expected: true,
		},
		{
			name:     "OTLP logs record, filter allows logs",
			allowed:  []string{definition.OTLPSignalLogs},
			signal:   definition.OTLPSignalLogs,
			expected: true,
		},
		{
			name:     "OTLP logs record, filter allows metrics only",
			allowed:  []string{definition.OTLPSignalMetrics},
			signal:   definition.OTLPSignalLogs,
			expected: false,
		},
		{
			name:     "OTLP traces record, filter allows logs and metrics",
			allowed:  []string{definition.OTLPSignalLogs, definition.OTLPSignalMetrics},
			signal:   definition.OTLPSignalTraces,
			expected: false,
		},
		{
			name:     "OTLP metrics record, multi-signal filter includes metrics",
			allowed:  []string{definition.OTLPSignalLogs, definition.OTLPSignalMetrics},
			signal:   definition.OTLPSignalMetrics,
			expected: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.expected, sinkAcceptsSignal(tc.allowed, tc.signal))
		})
	}
}
