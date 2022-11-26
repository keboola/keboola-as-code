package idgenerator_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	. "github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
)

func TestRequestId(t *testing.T) {
	t.Parallel()
	assert.Len(t, RequestID(), RequestIDLength)
}

func TestTemplateInstanceId(t *testing.T) {
	t.Parallel()
	assert.Len(t, TemplateInstanceID(), TemplateInstanceIDLength)
}

func TestEtcdNamespaceForE2ETest(t *testing.T) {
	t.Parallel()
	assert.Len(t, EtcdNamespaceForTest(), EtcdNamespaceForE2ETestLength)
}
