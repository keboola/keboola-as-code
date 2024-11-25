package ioutil

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewPrefixWriter(t *testing.T) {
	t.Parallel()
	out := NewAtomicWriter()
	w := NewPrefixWriter("[prefix]", out)

	_, err := w.Write([]byte("message 1\n"))
	require.NoError(t, err)

	_, err = w.Write([]byte("message 2\nmessage 3\nmessage 4\n"))
	require.NoError(t, err)

	assert.Equal(t, strings.TrimSpace(`
[prefix]message 1
[prefix]message 2
[prefix]message 3
[prefix]message 4
`), strings.TrimSpace(out.String()))
}
