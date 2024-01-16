package volume

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/test"
)

func TestSortVolumes(t *testing.T) {
	t.Parallel()

	volumes := []*test.Volume{
		{
			TypeValue:  "type2",
			LabelValue: "002",
		},
		{
			TypeValue:  "type1",
			LabelValue: "003",
		},
		{
			TypeValue:  "type1",
			LabelValue: "001",
		},
		{
			TypeValue:  "type2",
			LabelValue: "001",
		},
		{
			TypeValue:  "type1",
			LabelValue: "002",
		},
	}

	sortVolumes(volumes)

	assert.Equal(t, []*test.Volume{
		{
			TypeValue:  "type1",
			LabelValue: "001",
		},
		{
			TypeValue:  "type1",
			LabelValue: "002",
		},
		{
			TypeValue:  "type1",
			LabelValue: "003",
		},
		{
			TypeValue:  "type2",
			LabelValue: "001",
		},
		{
			TypeValue:  "type2",
			LabelValue: "002",
		},
	}, volumes)
}
