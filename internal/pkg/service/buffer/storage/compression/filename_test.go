package compression

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
			assert.Equal(t, tc.expected, filename)
		}
	}
}
