package dialog

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestApiHostValidator(t *testing.T) {
	t.Parallel()
	assert.NoError(t, StorageApiHostValidator("connection.keboola.com"))
	assert.NoError(t, StorageApiHostValidator("connection.keboola.com/"))
	assert.NoError(t, StorageApiHostValidator("https://connection.keboola.com"))
	assert.NoError(t, StorageApiHostValidator("https://connection.keboola.com/"))
	assert.Equal(t, errors.New("value is required"), StorageApiHostValidator(""))
	assert.Equal(t, errors.New("invalid host"), StorageApiHostValidator("@#$$%^&%#$&"))
}
