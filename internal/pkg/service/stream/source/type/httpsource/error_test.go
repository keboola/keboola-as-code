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
		path     string
		expected []attribute.KeyValue
	}{
		{path: "/stream/123/my-source/secret", expected: ids},
		{path: "/otlp/123/my-source/secret/v1/traces", expected: ids},
		{path: "/otlp/123/my-source/v1/traces", expected: ids},
		{path: "/stream/123", expected: nil},
		{path: "/health-check", expected: nil},
		{path: "/", expected: nil},
	}
	for _, tc := range cases {
		assert.Equal(t, tc.expected, sourceAttrs(tc.path), tc.path)
	}
}
