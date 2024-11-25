package compression

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFilename(t *testing.T) {
	t.Parallel()

	cases := []struct {
		expected string
		t        Type
	}{
		{"file.txt", TypeNone},
		{"file.txt.gz", TypeGZIP},
		{"file.txt.zstd", TypeZSTD},
		{"", "invalid"},
	}

	for _, tc := range cases {
		filename, err := Filename("file.txt", tc.t)
		if tc.expected == "" {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			assert.Equal(t, tc.expected, filename)
		}
	}
}
