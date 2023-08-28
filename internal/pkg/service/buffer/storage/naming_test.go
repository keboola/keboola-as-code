package storage

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/compression"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSliceFilename(t *testing.T) {
	t.Parallel()

	cases := []struct {
		expected string
		ft       FileType
		ct       compression.Type
	}{
		{"slice.csv", FileTypeCSV, compression.TypeNone},
		{"slice.csv.gzip", FileTypeCSV, compression.TypeGZIP},
		{"slice.csv.zstd", FileTypeCSV, compression.TypeZSTD},
		{"", "invalid", compression.TypeNone},
		{"", FileTypeCSV, "invalid"},
	}

	for _, tc := range cases {
		filename, err := SliceFilename(tc.ft, tc.ct)
		if tc.expected == "" {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
			assert.Equal(t, tc.expected, filename)
		}
	}
}
