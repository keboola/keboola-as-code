package httpsource

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/attribute"
)

func TestSourceAttrs(t *testing.T) {
	t.Parallel()

	ids := []attribute.KeyValue{attribute.String("project.id", "123"), attribute.String("source.id", "my-source")}
	cases := []struct {
		name     string
		path     string
		expected []attribute.KeyValue
	}{
		{name: "stream", path: "/stream/123/my-source/secret", expected: ids},
		{name: "otlp with secret in path", path: "/otlp/123/my-source/secret/v1/traces", expected: ids},
		{name: "otlp with secret in header", path: "/otlp/123/my-source/v1/traces", expected: ids},
		{name: "stream without source ID", path: "/stream/123/secret", expected: nil},
		{name: "stream with extra segment", path: "/stream/123/my-source/secret/extra", expected: nil},
		{name: "otlp without signal", path: "/otlp/123/my-source/secret/v1", expected: nil},
		{name: "other path", path: "/health-check", expected: nil},
		{name: "root", path: "/", expected: nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.expected, sourceAttrs(tc.path))
		})
	}
}
