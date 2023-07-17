// Package statistics provides:
// - Collecting of statistics from the API node import endpoint.
// - Caching of statistics used by of the upload and import conditions resolver.

package statistics

import "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/slicestate"

// nolint:gochecknoglobals
var allStates = []slicestate.State{
	slicestate.Writing, // 	includes Closing state, which is not stored separately
	slicestate.Uploading,
	slicestate.Uploaded,
	slicestate.Failed,
	slicestate.Imported,
}

func AllStates() []slicestate.State {
	return allStates[:]
}
