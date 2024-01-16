package volume_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/volume"
)

func TestCollection_All_SortVolumes(t *testing.T) {
	t.Parallel()

	c, err := volume.NewCollection[*test.Volume]([]*test.Volume{
		{
			IDValue:     "my-volume-5",
			NodeIDValue: "node-b",
			PathValue:   "type2/002",
			TypeValue:   "type2",
			LabelValue:  "002",
		},
		{
			IDValue:     "my-volume-3",
			NodeIDValue: "node-b",
			PathValue:   "type1/003",
			TypeValue:   "type1",
			LabelValue:  "003",
		},
		{
			IDValue:     "my-volume-1",
			NodeIDValue: "node-a",
			PathValue:   "type1/001",
			TypeValue:   "type1",
			LabelValue:  "001",
		},
		{
			IDValue:     "my-volume-4",
			NodeIDValue: "node-b",
			PathValue:   "type2/001",
			TypeValue:   "type2",
			LabelValue:  "001",
		},
		{
			IDValue:     "my-volume-2",
			NodeIDValue: "node-a",
			PathValue:   "type1/002",
			TypeValue:   "type1",
			LabelValue:  "002",
		},
	})
	require.NoError(t, err)

	assert.Equal(t, []*test.Volume{
		{
			IDValue:     "my-volume-1",
			NodeIDValue: "node-a",
			PathValue:   "type1/001",
			TypeValue:   "type1",
			LabelValue:  "001",
		},
		{
			IDValue:     "my-volume-2",
			NodeIDValue: "node-a",
			PathValue:   "type1/002",
			TypeValue:   "type1",
			LabelValue:  "002",
		},
		{
			IDValue:     "my-volume-3",
			NodeIDValue: "node-b",
			PathValue:   "type1/003",
			TypeValue:   "type1",
			LabelValue:  "003",
		},
		{
			IDValue:     "my-volume-4",
			NodeIDValue: "node-b",
			PathValue:   "type2/001",
			TypeValue:   "type2",
			LabelValue:  "001",
		},
		{
			IDValue:     "my-volume-5",
			NodeIDValue: "node-b",
			PathValue:   "type2/002",
			TypeValue:   "type2",
			LabelValue:  "002",
		},
	}, c.All())
}
