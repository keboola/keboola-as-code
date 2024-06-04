package model

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/compression"
)

func TestSliceFilename(t *testing.T) {
	t.Parallel()

	cases := []struct {
		expected string
		ft       FileType
		ct       compression.Type
	}{
		{"slice.csv", FileTypeCSV, compression.TypeNone},
		{"slice.csv.gz", FileTypeCSV, compression.TypeGZIP},
		{"slice.csv.zstd", FileTypeCSV, compression.TypeZSTD},
		{"", "invalid", compression.TypeNone},
		{"", FileTypeCSV, "invalid"},
	}

	for _, tc := range cases {
		filename, err := SliceFilename(tc.ft, tc.ct)
		if tc.expected == "" {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			assert.Equal(t, tc.expected, filename)
		}
	}
}
