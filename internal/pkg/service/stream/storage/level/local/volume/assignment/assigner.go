package assignment

import (
	"math/rand"
	"sort"
	"strconv"

	"github.com/cespare/xxhash/v2"

	volume "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
)

type Assignment struct {
	// Config is snapshot of the used assignment configuration.
	Config Config `json:"config"`
	// Volumes field contains assigned volumes to the file.
	Volumes []volume.ID `json:"volumes" validate:"min=1"`
}

// VolumesFor returns volumes list according to the file settings.
// See assignVolumes function for details.

// VolumesFor returns volumes list according to the file settings.
//
// The "preferredTypes" slice defines priority of the volumes types.
// The first value in the "preferredTypes" slice has the highest priority.
// The function tries to assign the maximum number of volumes from the following levels:
//   - the most preferred type, then
//   - less preferred types, then
//   - other types that are not on the preferred list
//
// Each selection is made from a different node, if possible.
//
// The "randomSeed" argument determines volume selection on the same priority level, and nodes order.
func VolumesFor(all []volume.Metadata, cfg Config, randomSeed int64) (out Assignment) {
	out.Config = cfg

	random := rand.New(rand.NewSource(randomSeed)) //nolint:gosec // weak random number generator is ok here

	// Convert preferred types to a map
	typePriority := newPriorityMap(cfg.PreferredTypes)

	// Shuffle volumes
	random.Shuffle(len(all), func(i, j int) { all[i], all[j] = all[j], all[i] })

	// Group volumes by node
	byNode := make(map[string][]volume.Metadata)
	for _, vol := range all {
		byNode[vol.NodeID] = append(byNode[vol.NodeID], vol)
	}

	// Convert map to slice and sort node volumes by the preferred priority.
	// Volumes with the most preferred volume type are first.
	perNode := make([]*nodeVolumes, 0, len(byNode))
	for nodeID, volumes := range byNode {
		typePriority.Sort(volumes, randomSeed)
		perNode = append(perNode, &nodeVolumes{nodeID: nodeID, volumes: newStack[volume.Metadata](volumes)})
	}

	// Shuffle nodes, map order above is random, so we must sort the slice at first
	sort.Slice(perNode, func(i, j int) bool { return perNode[i].nodeID < perNode[j].nodeID })
	random.Shuffle(len(perNode), func(i, j int) { perNode[i], perNode[j] = perNode[j], perNode[i] })

	// Get up to limit volumes according preferred types
	volTypes := newStack[string](cfg.PreferredTypes)
	volType, _ := volTypes.Pop()
	matchVolType := func(v volume.Metadata) bool { return volType == "" || v.Type == volType }
	for {
		found := false
		for _, node := range perNode {
			// Is limit reached?
			if len(out.Volumes) == cfg.Count {
				return out
			}

			// Get top volume from the node, if any
			if vol, ok := node.volumes.PopIf(matchVolType); ok {
				found = true
				out.Volumes = append(out.Volumes, vol.ID)
			}
		}

		if !found && volType == "" {
			// There is no next volume in all nodes, end
			return out
		}

		if !found {
			// Try less preferred volume type.
			// Volume type is set to an empty string, if the list is empty,
			// then matchVolType matches all volumes.
			volType, _ = volTypes.Pop()
			continue
		}

		// Continue with the same volume type
	}
}

type priorityMap map[volumeType]volumeTypePriority

type volumeType string

type volumeTypePriority int

// newPriorityMap constructs priority map from the preferred volumes types slices.
// The most preferred type has assigned the biggest number, the least type has assigned number "1".
// If an unspecified type that is not there is requested from the map, it has assigned number "0".
func newPriorityMap(preferred []string) priorityMap {
	priority := 1
	m := make(priorityMap)
	for i := len(preferred) - 1; i >= 0; i-- {
		m[volumeType(preferred[i])] = volumeTypePriority(priority)
		priority++
	}
	return m
}

// Sort volumes by the preferred types.
func (m priorityMap) Sort(volumes []volume.Metadata, randomSeed int64) {
	randomStr := strconv.FormatInt(randomSeed, 10)
	sort.SliceStable(volumes, func(i, j int) bool {
		// If the "type" key is not found in the map,
		// the empty value (0, the lowest priority) is returned from the map.
		iPriority := m[volumeType(volumes[i].Type)]
		jPriority := m[volumeType(volumes[j].Type)]
		if iPriority != jPriority {
			return iPriority > jPriority
		}

		// Sort volumes on the same level: randomly, but stably.
		// For the same input parameters of the function assignVolumes,
		// the same order is always generated.
		iHash := xxhash.Sum64String(randomStr + volumes[i].Type + volumes[i].Label)
		jHash := xxhash.Sum64String(randomStr + volumes[j].Type + volumes[j].Label)
		return iHash < jHash
	})
}

type nodeVolumes struct {
	nodeID  string
	volumes *stack[volume.Metadata]
}

type stack[T any] struct {
	values []T
}

func newStack[T any](values []T) *stack[T] {
	return &stack[T]{values: values}
}

func (s *stack[T]) Push(value T) {
	s.values = append(s.values, value)
}

func (s *stack[T]) Pop() (T, bool) {
	return s.PopIf(nil)
}

func (s *stack[T]) PopIf(cond func(v T) bool) (T, bool) {
	var empty T

	// Is empty?
	if len(s.values) == 0 {
		return empty, false
	}

	// Is condition meet?
	out := s.values[0]
	if cond != nil && !cond(out) {
		return empty, false
	}

	// Pop
	s.values = s.values[1:]
	return out, true
}
