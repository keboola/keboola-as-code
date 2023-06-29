package task

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func TestUserError(t *testing.T) {
	t.Parallel()
	assert.False(t, isUserError(errors.New("some error")))
	assert.True(t, isUserError(WrapUserError(errors.New("some error"))))
}
