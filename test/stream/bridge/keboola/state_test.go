package keboola_test

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
)

type records struct {
	startID int
	count   int
}

type file struct {
	state   model.FileState
	volumes []volume
}

type fileWithSlices struct {
	file   model.File
	slices []model.Slice
}

type volume struct {
	slices []model.SliceState
}

type sliceUpload struct {
	records       records
	expectedFiles []file
}

type fileImport struct {
	sendRecords          records
	expectedFileRecords  records
	expectedTableRecords records
	expectedFiles        []file
}

// checkState compares expected and actual states of files and slices.
func (ts *testState) checkState(t *testing.T, ctx context.Context, expected []file) (files []fileWithSlices) {
	t.Helper()

	// Load entities
	files = ts.getFilesAndSlices(t, ctx)

	// Group entities from the actual state
	actual := make([]file, 0, len(expected))
	volumesIndexMap := make(map[model.FileVolumeKey]int)
	volumesMap := make(map[model.FileKey][]volume)
	for _, f := range files {
		for _, s := range f.slices {
			// Get/init volume in the slice
			volumes := volumesMap[s.FileKey]

			// Generate some aux volume ID to group slices
			volIndex, ok := volumesIndexMap[s.FileVolumeKey]
			if !ok {
				volIndex = len(volumes)
				volumes = append(volumes, volume{})
				volumesIndexMap[s.FileVolumeKey] = volIndex
			}

			// Append slice
			vol := volumes[volIndex]
			vol.slices = append(vol.slices, s.State)

			// Update
			volumes[volIndex] = vol
			volumesMap[s.FileKey] = volumes
		}
	}
	for _, f := range files {
		actual = append(actual, file{state: f.file.State, volumes: volumesMap[f.file.FileKey]})
	}

	// Compare
	require.Equal(t, expected, actual)

	return files
}

// getFilesAndSlices returns file with its slices, both are sorted by OpenedAt timestamp.
func (ts *testState) getFilesAndSlices(t *testing.T, ctx context.Context) (out []fileWithSlices) {
	t.Helper()

	// Get files
	files, err := ts.apiScp.StorageRepository().File().ListIn(ts.sinkKey).Do(ctx).All()
	require.NoError(t, err)

	// Get slices
	for _, f := range files {
		fs := fileWithSlices{file: f}

		// Get slices
		fs.slices, err = ts.apiScp.StorageRepository().Slice().ListIn(f.FileKey).Do(ctx).All()
		require.NoError(t, err)

		// Sort slices by <openedAt, volumeID>, from DB they are sorted by <volumeID, openedAt>
		slices.SortStableFunc(fs.slices, func(a, b model.Slice) int {
			timeDiff := int(a.OpenedAt().Time().Unix() - b.OpenedAt().Time().Unix())
			if timeDiff != 0 {
				return timeDiff
			}
			return strings.Compare(a.VolumeID.String(), b.VolumeID.String())
		})

		out = append(out, fs)
	}

	return out
}
