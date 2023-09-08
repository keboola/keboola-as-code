package writer

import (
	"context"
	"sort"

	"github.com/benbjohnson/clock"
	"github.com/cespare/xxhash/v2"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/local/volume"
)

type baseVolumes = volume.Volumes[*Volume]

type Volumes struct {
	*baseVolumes
}

func OpenVolumes(ctx context.Context, logger log.Logger, clock clock.Clock, path string, opts ...Option) (*Volumes, error) {
	v, err := volume.OpenVolumes(logger, path, func(info volumeInfo) (*Volume, error) {
		return OpenVolume(ctx, logger, clock, info, opts...)
	})
	if err != nil {
		return nil, err
	}

	return &Volumes{baseVolumes: v}, nil
}

// VolumesFor returns volumes list according to the file settings.
// One slice of the file should be written simultaneously to each volume.
func (v *Volumes) VolumesFor(file *storage.File) []*Volume {
	return v.assignVolumes(
		file.LocalStorage.Volumes.PerPod,
		file.LocalStorage.Volumes.PreferredTypes,
		file.OpenedAt().String(),
	)
}

// assignVolumes returns the requested number of volumes, if so many volumes are available.
//
// The "preferredTypes" slice defines priority of the volumes types.
// The first value in the slice has the highest priority.
// The function returns the maximum number of volumes:
//   - of the most preferred type, then
//   - of less preferred types, then
//   - of other types in lexicographic order.
//
// The "randomFed" argument determines volume selection on the same priority level.
func (v *Volumes) assignVolumes(count int, preferredTypes []string, randomFed string) (out []*Volume) {
	// Convert preferred slice to a map
	priority := 1
	priorityByType := make(map[string]int)
	for i := len(preferredTypes) - 1; i >= 0; i-- {
		priorityByType[preferredTypes[i]] = priority
		priority++
	}

	// Sort volumes
	volumes := v.allExceptDrained()
	sort.SliceStable(volumes, func(i, j int) bool {
		// Sort volumes by the preferred types.
		// If the "type" key is not found in the priorityByType map, the empty value (0) is returned.
		iPriority := priorityByType[volumes[i].Type()]
		jPriority := priorityByType[volumes[j].Type()]
		if iPriority != jPriority {
			// Higher the priority = higher the position in the list
			return iPriority > jPriority
		}

		// Volumes with the same priority sort by the label.
		// Use a randomFed to make the distribution of the volumes more even.
		iHash := xxhash.Sum64String(randomFed + volumes[i].Type() + volumes[i].Label())
		jHash := xxhash.Sum64String(randomFed + volumes[j].Type() + volumes[j].Label())
		return iHash < jHash
	})

	// Check boundary
	if count > len(volumes) {
		count = len(volumes)
	}

	// Return first N volumes
	return volumes[:count]
}

func (v *Volumes) allExceptDrained() (out []*Volume) {
	for _, vol := range v.All() {
		if !vol.Drained() {
			out = append(out, vol)
		}
	}
	return out
}
