package assignment_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network/router/assignment"
	volume "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

type assignTestCase struct {
	Name               string
	Description        string
	SourceNodes        []string
	Volumes            []volume.ID
	MinSlicesPerNode   int
	ExpectedAssignment map[string][]volume.ID
}

func TestAssignSlices(t *testing.T) {
	t.Parallel()

	cases := []assignTestCase{
		{
			Name:        "1 source, 1 volume",
			SourceNodes: []string{"source1"},
			Volumes:     []volume.ID{"volume1"},
			ExpectedAssignment: map[string][]volume.ID{
				"source1": {"volume1"},
			},
		},
		{
			Name:        "1 source, 2 volumes",
			SourceNodes: []string{"source1"},
			Volumes:     []volume.ID{"volume1", "volume2"},
			ExpectedAssignment: map[string][]volume.ID{
				"source1": {"volume1", "volume2"},
			},
		},
		{
			Name:        "1 source, 3 volumes",
			SourceNodes: []string{"source1"},
			Volumes:     []volume.ID{"volume1", "volume2", "volume3"},
			ExpectedAssignment: map[string][]volume.ID{
				"source1": {"volume1", "volume2", "volume3"},
			},
		},
		{
			Name:        "2 sources, 1 volume",
			SourceNodes: []string{"source1", "source2"},
			Volumes:     []volume.ID{"volume1"},
			ExpectedAssignment: map[string][]volume.ID{
				"source1": {"volume1"},
				"source2": {"volume1"},
			},
		},
		{
			Name:        "2 sources, 2 volumes",
			SourceNodes: []string{"source1", "source2"},
			Volumes:     []volume.ID{"volume1", "volume2"},
			ExpectedAssignment: map[string][]volume.ID{
				"source1": {"volume1"},
				"source2": {"volume2"},
			},
		},
		{
			Name:        "3 sources, 1 volume",
			SourceNodes: []string{"source1", "source2", "source3"},
			Volumes:     []volume.ID{"volume1"},
			ExpectedAssignment: map[string][]volume.ID{
				"source1": {"volume1"},
				"source2": {"volume1"},
				"source3": {"volume1"},
			},
		},
		{
			Name:        "3 sources, 2 volumes",
			SourceNodes: []string{"source1", "source2", "source3"},
			Volumes:     []volume.ID{"volume1", "volume2"},
			ExpectedAssignment: map[string][]volume.ID{
				"source1": {"volume1"},
				"source2": {"volume2"},
				"source3": {"volume1"},
			},
		},
		{
			Name:        "3 sources, 3 volumes",
			SourceNodes: []string{"source1", "source2", "source3"},
			Volumes:     []volume.ID{"volume1", "volume2", "volume3"},
			ExpectedAssignment: map[string][]volume.ID{
				"source1": {"volume1"},
				"source2": {"volume2"},
				"source3": {"volume3"},
			},
		},
		{
			Name:             "3 sources, 3 volumes, min 2 per node",
			SourceNodes:      []string{"source1", "source2", "source3"},
			Volumes:          []volume.ID{"volume1", "volume2", "volume3"},
			MinSlicesPerNode: 2,
			ExpectedAssignment: map[string][]volume.ID{
				"source1": {"volume1", "volume2"},
				"source2": {"volume3", "volume1"},
				"source3": {"volume2", "volume3"},
			},
		},
		{
			Name:        "8 sources, 3 volumes",
			SourceNodes: []string{"source1", "source2", "source3", "source4", "source5", "source6", "source7", "source8"},
			Volumes:     []volume.ID{"volume1", "volume2", "volume3"},
			ExpectedAssignment: map[string][]volume.ID{
				"source1": {"volume1"},
				"source2": {"volume2"},
				"source3": {"volume3"},
				"source4": {"volume1"},
				"source5": {"volume2"},
				"source6": {"volume3"},
				"source7": {"volume1"},
				"source8": {"volume2"},
			},
		},
		{
			Name:        "3 sources, 8 volumes",
			SourceNodes: []string{"source1", "source2", "source3"},
			Volumes:     []volume.ID{"volume1", "volume2", "volume3", "volume4", "volume5", "volume6", "volume7", "volume8"},
			ExpectedAssignment: map[string][]volume.ID{
				"source1": {"volume1", "volume2", "volume3"},
				"source2": {"volume4", "volume5", "volume6"},
				"source3": {"volume7", "volume8", "volume1"},
			},
		},
		{
			Name:             "3 sources, 8 volumes, min 2 per node",
			Description:      "MinSlicesPerNode is smaller than average slices count per node, so it is not used",
			SourceNodes:      []string{"source1", "source2", "source3"},
			Volumes:          []volume.ID{"volume1", "volume2", "volume3", "volume4", "volume5", "volume6", "volume7", "volume8"},
			MinSlicesPerNode: 2,
			ExpectedAssignment: map[string][]volume.ID{
				"source1": {"volume1", "volume2", "volume3"},
				"source2": {"volume4", "volume5", "volume6"},
				"source3": {"volume7", "volume8", "volume1"},
			},
		},
		{
			Name:             "3 sources, 8 volumes, min 5 per node",
			SourceNodes:      []string{"source1", "source2", "source3"},
			Volumes:          []volume.ID{"volume1", "volume2", "volume3", "volume4", "volume5", "volume6", "volume7", "volume8"},
			MinSlicesPerNode: 5,
			ExpectedAssignment: map[string][]volume.ID{
				"source1": {"volume1", "volume2", "volume3", "volume4", "volume5"},
				"source2": {"volume6", "volume7", "volume8", "volume1", "volume2"},
				"source3": {"volume3", "volume4", "volume5", "volume6", "volume7"},
			},
		},
	}

	for _, tc := range cases {
		tcName := strhelper.NormalizeName(tc.Name)
		t.Run(tcName, func(t *testing.T) {
			t.Parallel()

			// Generate one slice for each specified volume
			var slices []model.SliceKey
			{
				sinkKey := test.NewSinkKey()
				openedAt := utctime.MustParse("2000-01-01T20:00:00.000Z")
				for _, volumeID := range tc.Volumes {
					sliceKey := model.SliceKey{
						FileVolumeKey: model.FileVolumeKey{
							FileKey:  model.FileKey{SinkKey: sinkKey, FileID: model.FileID{OpenedAt: openedAt}},
							VolumeID: volumeID,
						},
						SliceID: model.SliceID{OpenedAt: openedAt},
					}
					slices = append(slices, sliceKey)
				}
			}

			// Assign volumes to source nodes and check results
			actualAssignment := make(map[string][]volume.ID)
			for _, nodeID := range tc.SourceNodes {
				var volumes []volume.ID
				for _, sliceKey := range assignment.AssignSlices(slices, tc.SourceNodes, nodeID, tc.MinSlicesPerNode) {
					volumes = append(volumes, sliceKey.VolumeID)
				}
				actualAssignment[nodeID] = volumes
			}
			assert.Equal(t, tc.ExpectedAssignment, actualAssignment)
		})
	}
}
