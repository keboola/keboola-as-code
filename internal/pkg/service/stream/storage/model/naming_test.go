package model

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/encoder"
)

func TestSliceFilename(t *testing.T) {
	t.Parallel()

	cases := []struct {
		expected string
		ft       encoder.Type
		ct       compression.Type
	}{
		{"slice.csv", encoder.TypeCSV, compression.TypeNone},
		{"slice.csv.gz", encoder.TypeCSV, compression.TypeGZIP},
		{"slice.csv.zstd", encoder.TypeCSV, compression.TypeZSTD},
		{"", "invalid", compression.TypeNone},
		{"", encoder.TypeCSV, "invalid"},
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
