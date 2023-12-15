package errors

import (
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/stretchr/testify/assert"
	"testing"
)

func AssertErrorStatusCode(t *testing.T, expectedStatusCode int, err error) {
	t.Helper()
	var errWithStatus WithStatusCode
	if assert.True(t, errors.As(err, &errWithStatus)) {
		assert.Equal(t, expectedStatusCode, errWithStatus.StatusCode())
	}
}
