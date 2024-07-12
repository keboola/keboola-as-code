package assignment_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/assignment"
	volume "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

type assignVolumesTestCase struct {
	Name            string
	Count           int
	PreferredTypes  []string
	FileOpenedAt    utctime.UTCTime
	AllVolumes      []string
	ExpectedVolumes []string
}

func TestVolumes_VolumesFor(t *testing.T) {
	t.Parallel()

	// Random fed determines volume selection on the same priority level.
	randomSeed1 := utctime.MustParse("2000-01-01T01:00:00.000Z")
	randomSeed2 := utctime.MustParse("2000-01-01T02:00:00.123Z")
	randomSeed3 := utctime.MustParse("2000-01-01T03:00:00.456Z")

	cases := []assignVolumesTestCase{
		{
			Name:            "empty",
			Count:           1,
			PreferredTypes:  []string{"missing"},
			FileOpenedAt:    randomSeed1,
			AllVolumes:      []string{},
			ExpectedVolumes: []string{},
		},
		{
			Name:            "nodes=1,count=1,pref=-,simple",
			Count:           1,
			PreferredTypes:  []string{"missing"},
			FileOpenedAt:    randomSeed1,
			AllVolumes:      []string{"node/hdd/1"},
			ExpectedVolumes: []string{"node/hdd/1"},
		},
		{
			Name:            "nodes=1,count=1,pref=-,rand=1",
			Count:           1,
			PreferredTypes:  []string{"missing"},
			FileOpenedAt:    randomSeed1,
			AllVolumes:      []string{"node/hdd/1", "node/hdd/2", "node/hdd/3", "node/ssd/1", "node/ssd/2", "node/top/1"},
			ExpectedVolumes: []string{"node/top/1"},
		},
		{
			Name:            "nodes=1,count=1,pref=-,rand=2",
			Count:           1,
			PreferredTypes:  []string{"missing"},
			FileOpenedAt:    randomSeed2,
			AllVolumes:      []string{"node/hdd/1", "node/hdd/2", "node/hdd/3", "node/ssd/1", "node/ssd/2", "node/top/1"},
			ExpectedVolumes: []string{"node/ssd/1"},
		},
		{
			Name:            "nodes=1,count=1,pref=top,rand=1",
			Count:           1,
			PreferredTypes:  []string{"top"},
			FileOpenedAt:    randomSeed1,
			AllVolumes:      []string{"node/hdd/1", "node/hdd/2", "node/hdd/3", "node/ssd/1", "node/ssd/2", "node/top/1"},
			ExpectedVolumes: []string{"node/top/1"},
		},
		{
			Name:            "nodes=1,count=1,pref=top,rand=2",
			Count:           1,
			PreferredTypes:  []string{"top"},
			FileOpenedAt:    randomSeed2,
			AllVolumes:      []string{"node/hdd/1", "node/hdd/2", "node/hdd/3", "node/ssd/1", "node/ssd/2", "node/top/1"},
			ExpectedVolumes: []string{"node/top/1"},
		},
		{
			Name:            "nodes=1,count=1,pref=hdd,rand=1",
			Count:           1,
			PreferredTypes:  []string{"hdd"},
			FileOpenedAt:    randomSeed1,
			AllVolumes:      []string{"node/hdd/1", "node/hdd/2", "node/hdd/3", "node/ssd/1", "node/ssd/2", "node/top/1"},
			ExpectedVolumes: []string{"node/hdd/1"},
		},
		{
			Name:            "nodes=1,count=1,pref=hdd,rand=2",
			Count:           1,
			PreferredTypes:  []string{"hdd"},
			FileOpenedAt:    randomSeed2,
			AllVolumes:      []string{"node/hdd/1", "node/hdd/2", "node/hdd/3", "node/ssd/1", "node/ssd/2", "node/top/1"},
			ExpectedVolumes: []string{"node/hdd/2"},
		},
		{
			Name:            "nodes=1,count=3,pref=-,simple",
			Count:           3,
			PreferredTypes:  []string{"missing"},
			FileOpenedAt:    randomSeed1,
			AllVolumes:      []string{"node/hdd/1", "node/ssd/1", "node/ssd/2"},
			ExpectedVolumes: []string{"node/hdd/1", "node/ssd/1", "node/ssd/2"},
		},
		{
			Name:            "nodes=1,count=3,pref=-,rand=1",
			Count:           3,
			PreferredTypes:  []string{"missing"},
			FileOpenedAt:    randomSeed1,
			AllVolumes:      []string{"node/hdd/1", "node/hdd/2", "node/hdd/3", "node/ssd/1", "node/ssd/2", "node/top/1"},
			ExpectedVolumes: []string{"node/top/1", "node/hdd/1", "node/hdd/3"},
		},
		{
			Name:            "nodes=1,count=3,pref=-,rand=2",
			Count:           3,
			PreferredTypes:  []string{"missing"},
			FileOpenedAt:    randomSeed2,
			AllVolumes:      []string{"node/hdd/1", "node/hdd/2", "node/hdd/3", "node/ssd/1", "node/ssd/2", "node/top/1"},
			ExpectedVolumes: []string{"node/ssd/1", "node/ssd/2", "node/hdd/2"},
		},
		{
			Name:            "nodes=1,count=3,pref=top,rand=1",
			Count:           3,
			PreferredTypes:  []string{"top"},
			FileOpenedAt:    randomSeed1,
			AllVolumes:      []string{"node/hdd/1", "node/hdd/2", "node/hdd/3", "node/ssd/1", "node/ssd/2", "node/top/1"},
			ExpectedVolumes: []string{"node/top/1", "node/hdd/1", "node/hdd/3"},
		},
		{
			Name:            "nodes=1,count=3,pref=top,rand=2",
			Count:           3,
			PreferredTypes:  []string{"top"},
			FileOpenedAt:    randomSeed2,
			AllVolumes:      []string{"node/hdd/1", "node/hdd/2", "node/hdd/3", "node/ssd/1", "node/ssd/2", "node/top/1"},
			ExpectedVolumes: []string{"node/top/1", "node/ssd/1", "node/ssd/2"},
		},
		{
			Name:            "nodes=1,count=3,pref=ssd,rand=1",
			Count:           3,
			PreferredTypes:  []string{"ssd"},
			FileOpenedAt:    randomSeed1,
			AllVolumes:      []string{"node/hdd/1", "node/hdd/2", "node/hdd/3", "node/ssd/1", "node/ssd/2", "node/top/1"},
			ExpectedVolumes: []string{"node/ssd/1", "node/ssd/2", "node/top/1"},
		},
		{
			Name:            "nodes=1,count=3,pref=ssd,rand=2",
			Count:           3,
			PreferredTypes:  []string{"ssd"},
			FileOpenedAt:    randomSeed2,
			AllVolumes:      []string{"node/hdd/1", "node/hdd/2", "node/hdd/3", "node/ssd/1", "node/ssd/2", "node/top/1"},
			ExpectedVolumes: []string{"node/ssd/1", "node/ssd/2", "node/hdd/2"},
		},
		{
			Name:            "nodes=1,count=4,pref=ssd,hdd,rand=1",
			Count:           4,
			PreferredTypes:  []string{"ssd", "hdd"},
			FileOpenedAt:    randomSeed1,
			AllVolumes:      []string{"node/hdd/1", "node/hdd/2", "node/hdd/3", "node/ssd/1", "node/ssd/2", "node/top/1"},
			ExpectedVolumes: []string{"node/ssd/1", "node/ssd/2", "node/hdd/1", "node/hdd/3"},
		},
		{
			Name:            "nodes=1,count=4,pref=ssd,hdd,rand=2",
			Count:           4,
			PreferredTypes:  []string{"ssd", "hdd"},
			FileOpenedAt:    randomSeed2,
			AllVolumes:      []string{"node/hdd/1", "node/hdd/2", "node/hdd/3", "node/ssd/1", "node/ssd/2", "node/top/1"},
			ExpectedVolumes: []string{"node/ssd/1", "node/ssd/2", "node/hdd/2", "node/hdd/3"},
		},
		{
			Name:            "nodes=1,count=4,pref=ssd,hdd,rand=3",
			Count:           4,
			PreferredTypes:  []string{"ssd", "hdd"},
			FileOpenedAt:    randomSeed3,
			AllVolumes:      []string{"node/hdd/1", "node/hdd/2", "node/hdd/3", "node/ssd/1", "node/ssd/2", "node/top/1"},
			ExpectedVolumes: []string{"node/ssd/2", "node/ssd/1", "node/hdd/1", "node/hdd/3"},
		},
		{
			Name:            "nodes=1,count=10,pref=-,rand=1",
			Count:           10,
			PreferredTypes:  []string{"missing"},
			FileOpenedAt:    randomSeed1,
			AllVolumes:      []string{"node/hdd/1", "node/hdd/2", "node/hdd/3", "node/ssd/1", "node/ssd/2", "node/top/1"},
			ExpectedVolumes: []string{"node/top/1", "node/hdd/1", "node/hdd/3", "node/ssd/1", "node/ssd/2", "node/hdd/2"},
		},
		{
			Name:            "nodes=1,count=10,pref=-,rand=2",
			Count:           10,
			PreferredTypes:  []string{"missing"},
			FileOpenedAt:    randomSeed2,
			AllVolumes:      []string{"node/hdd/1", "node/hdd/2", "node/hdd/3", "node/ssd/1", "node/ssd/2", "node/top/1"},
			ExpectedVolumes: []string{"node/ssd/1", "node/ssd/2", "node/hdd/2", "node/hdd/3", "node/hdd/1", "node/top/1"},
		},
		{
			Name:            "nodes=1,count=10,pref=top,hdd,rand=1",
			Count:           10,
			PreferredTypes:  []string{"top", "hdd"},
			FileOpenedAt:    randomSeed1,
			AllVolumes:      []string{"node/hdd/1", "node/hdd/2", "node/hdd/3", "node/ssd/1", "node/ssd/2", "node/top/1"},
			ExpectedVolumes: []string{"node/top/1", "node/hdd/1", "node/hdd/3", "node/hdd/2", "node/ssd/1", "node/ssd/2"},
		},
		{
			Name:            "nodes=1,count=10,pref=top,hdd,rand=2",
			Count:           10,
			PreferredTypes:  []string{"top", "hdd"},
			FileOpenedAt:    randomSeed2,
			AllVolumes:      []string{"node/hdd/1", "node/hdd/2", "node/hdd/3", "node/ssd/1", "node/ssd/2", "node/top/1"},
			ExpectedVolumes: []string{"node/top/1", "node/hdd/2", "node/hdd/3", "node/hdd/1", "node/ssd/1", "node/ssd/2"},
		},
		{
			Name:            "nodes=1,count=10,pref=top,hdd,rand=3",
			Count:           10,
			PreferredTypes:  []string{"top", "hdd"},
			FileOpenedAt:    randomSeed3,
			AllVolumes:      []string{"node/hdd/1", "node/hdd/2", "node/hdd/3", "node/ssd/1", "node/ssd/2", "node/top/1"},
			ExpectedVolumes: []string{"node/top/1", "node/hdd/1", "node/hdd/3", "node/hdd/2", "node/ssd/2", "node/ssd/1"},
		},
		{
			Name:            "nodes=3,count=1,pref=-,rand=1",
			Count:           1,
			PreferredTypes:  []string{"missing"},
			FileOpenedAt:    randomSeed1,
			AllVolumes:      []string{"node1/hdd/1", "node1/hdd/2", "node1/hdd/3", "node2/ssd/1", "node3/ssd/2", "node2/top/1"},
			ExpectedVolumes: []string{"node3/ssd/2"},
		},
		{
			Name:            "nodes=3,count=1,pref=-,rand=2",
			Count:           1,
			PreferredTypes:  []string{"missing"},
			FileOpenedAt:    randomSeed2,
			AllVolumes:      []string{"node1/hdd/1", "node1/hdd/2", "node1/hdd/3", "node2/ssd/1", "node3/ssd/2", "node2/top/1"},
			ExpectedVolumes: []string{"node2/ssd/1"},
		},
		{
			Name:            "nodes=3,count=1,pref=-,rand=3",
			Count:           1,
			PreferredTypes:  []string{"missing"},
			FileOpenedAt:    randomSeed3,
			AllVolumes:      []string{"node1/hdd/1", "node1/hdd/2", "node1/hdd/3", "node2/ssd/1", "node3/ssd/2", "node2/top/1"},
			ExpectedVolumes: []string{"node3/ssd/2"},
		},
		{
			Name:            "nodes=3,count=3,pref=-,rand=1",
			Count:           3,
			PreferredTypes:  []string{"missing"},
			FileOpenedAt:    randomSeed1,
			AllVolumes:      []string{"node1/hdd/1", "node1/hdd/2", "node1/hdd/3", "node2/ssd/1", "node3/ssd/2", "node2/top/1"},
			ExpectedVolumes: []string{"node3/ssd/2", "node1/hdd/1", "node2/top/1"},
		},
		{
			Name:            "nodes=3,count=3,pref=-,rand=2",
			Count:           3,
			PreferredTypes:  []string{"missing"},
			FileOpenedAt:    randomSeed2,
			AllVolumes:      []string{"node1/hdd/1", "node1/hdd/2", "node1/hdd/3", "node2/ssd/1", "node3/ssd/2", "node2/top/1"},
			ExpectedVolumes: []string{"node2/ssd/1", "node1/hdd/2", "node3/ssd/2"},
		},
		{
			Name:            "nodes=3,count=3,pref=-,rand=3",
			Count:           3,
			PreferredTypes:  []string{"missing"},
			FileOpenedAt:    randomSeed3,
			AllVolumes:      []string{"node1/hdd/1", "node1/hdd/2", "node1/hdd/3", "node2/ssd/1", "node3/ssd/2", "node2/top/1"},
			ExpectedVolumes: []string{"node3/ssd/2", "node1/hdd/1", "node2/top/1"},
		},
		{
			Name:            "nodes=3,count=10,pref=-,rand=1",
			Count:           10,
			PreferredTypes:  []string{"missing"},
			FileOpenedAt:    randomSeed1,
			AllVolumes:      []string{"node1/hdd/1", "node1/hdd/2", "node1/hdd/3", "node2/ssd/1", "node3/ssd/2", "node2/top/1"},
			ExpectedVolumes: []string{"node3/ssd/2", "node1/hdd/1", "node2/top/1", "node1/hdd/3", "node2/ssd/1", "node1/hdd/2"},
		},
		{
			Name:            "nodes=3,count=10,pref=-,rand=2",
			Count:           10,
			PreferredTypes:  []string{"missing"},
			FileOpenedAt:    randomSeed2,
			AllVolumes:      []string{"node1/hdd/1", "node1/hdd/2", "node1/hdd/3", "node2/ssd/1", "node3/ssd/2", "node2/top/1"},
			ExpectedVolumes: []string{"node2/ssd/1", "node1/hdd/2", "node3/ssd/2", "node2/top/1", "node1/hdd/3", "node1/hdd/1"},
		},
		{
			Name:            "nodes=3,count=10,pref=-,rand=3",
			Count:           10,
			PreferredTypes:  []string{"missing"},
			FileOpenedAt:    randomSeed3,
			AllVolumes:      []string{"node1/hdd/1", "node1/hdd/2", "node1/hdd/3", "node2/ssd/1", "node3/ssd/2", "node2/top/1"},
			ExpectedVolumes: []string{"node3/ssd/2", "node1/hdd/1", "node2/top/1", "node1/hdd/3", "node2/ssd/1", "node1/hdd/2"},
		},
		{
			Name:            "nodes=5,count=7,pref=ssd,rand=1",
			Count:           7,
			PreferredTypes:  []string{"ssd"},
			FileOpenedAt:    randomSeed1,
			AllVolumes:      []string{"node1/hdd/1", "node3/ssd/2", "node2/hdd/3", "node2/ssd/1", "node4/ssd/2", "node2/hdd/1", "node5/ssd/1", "node1/hdd/2", "node5/ssd/2"},
			ExpectedVolumes: []string{"node3/ssd/2", "node4/ssd/2", "node2/ssd/1", "node5/ssd/1", "node5/ssd/2", "node2/hdd/1", "node1/hdd/1"},
		},
		{
			Name:            "nodes=5,count=7,pref=ssd,rand=2",
			Count:           7,
			PreferredTypes:  []string{"ssd"},
			FileOpenedAt:    randomSeed2,
			AllVolumes:      []string{"node1/hdd/1", "node3/ssd/2", "node2/hdd/3", "node2/ssd/1", "node4/ssd/2", "node2/hdd/1", "node5/ssd/1", "node1/hdd/2", "node5/ssd/2"},
			ExpectedVolumes: []string{"node3/ssd/2", "node2/ssd/1", "node5/ssd/1", "node4/ssd/2", "node5/ssd/2", "node2/hdd/3", "node1/hdd/2"},
		},
		{
			Name:            "nodes=5,count=7,pref=ssd,rand=3",
			Count:           7,
			PreferredTypes:  []string{"ssd"},
			FileOpenedAt:    randomSeed3,
			AllVolumes:      []string{"node1/hdd/1", "node3/ssd/2", "node2/hdd/3", "node2/ssd/1", "node4/ssd/2", "node2/hdd/1", "node5/ssd/1", "node1/hdd/2", "node5/ssd/2"},
			ExpectedVolumes: []string{"node5/ssd/2", "node3/ssd/2", "node4/ssd/2", "node2/ssd/1", "node5/ssd/1", "node2/hdd/1", "node1/hdd/1"},
		},
	}

	// Run test cases
	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()

			// Create volumes
			volumes, byID := createVolumesMetadata(t, tc.AllVolumes)

			// Create assigner config
			cfg := assignment.Config{
				Count:          tc.Count,
				PreferredTypes: tc.PreferredTypes,
			}

			// Config must be valid
			require.NoError(t, validator.New().Validate(context.Background(), cfg))

			// Assign volumes
			result := assignment.VolumesFor(volumes, cfg, tc.FileOpenedAt.Time().UnixNano())

			// Get IDs of the assigned volumes
			actualVolumes := make([]string, len(result.Volumes))
			for i, volumeID := range result.Volumes {
				vol := byID[volumeID]
				assert.NotEmpty(t, vol)
				actualVolumes[i] = fmt.Sprintf(`%s/%s/%s`, vol.NodeID, vol.Type, vol.Label)
			}

			// Compare
			assert.Equal(t, tc.ExpectedVolumes, actualVolumes)
		})
	}
}

func createVolumesMetadata(t *testing.T, volumes []string) (all []volume.Metadata, byID map[volume.ID]volume.Metadata) {
	t.Helper()

	byID = make(map[volume.ID]volume.Metadata)
	for _, definition := range volumes {
		parts := strings.Split(definition, "/")
		require.Len(t, parts, 3, "volume definition must have 3 parts: <node>/<type>/<label>")

		id := volume.GenerateID()
		metadata := volume.Metadata{
			ID: id,
			Spec: volume.Spec{
				NodeID:   parts[0],
				Hostname: "localhost",
				Type:     parts[1],
				Label:    parts[2],
			},
		}
		byID[id] = metadata
		all = append(all, metadata)
	}

	return all, byID
}
