package idgenerator_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	. "github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
)

func TestRequestId(t *testing.T) {
	t.Parallel()
	assert.Len(t, RequestId(), RequestIdLength)
}

func TestTemplateInstanceId(t *testing.T) {
	t.Parallel()
	assert.Len(t, TemplateInstanceId(), TemplateInstanceIdLength)
}

func TestEtcdNamespaceForE2ETest(t *testing.T) {
	t.Parallel()
	assert.Len(t, EtcdNamespaceForTest(), EtcdNamespaceForE2ETestLength)
}
