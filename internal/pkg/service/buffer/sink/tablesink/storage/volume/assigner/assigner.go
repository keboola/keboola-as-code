package assigner

import (
	"math/rand"
	"sort"
	"strconv"

	"github.com/cespare/xxhash/v2"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage"
)

// VolumesFor returns volumes list according to the file settings.
// See assignVolumes function for details.
func VolumesFor(volumes []storage.VolumeMetadata, file storage.File) ([]storage.VolumeMetadata, error) {
	return assignVolumes(
		volumes,
		file.LocalStorage.Volumes.Count,
		file.LocalStorage.Volumes.PreferredTypes,
		file.OpenedAt().Time().UnixNano(),
	), nil
}

// assignVolumes returns the requested number of volumes, if so many volumes are available.
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
func assignVolumes(input []storage.VolumeMetadata, limit int, preferredTypes []string, randomSeed int64) (out []storage.VolumeMetadata) {
	random := rand.New(rand.NewSource(randomSeed)) //nolint:gosec // weak random number generator is ok here

	// Convert preferred types to a map
	typePriority := newPriorityMap(preferredTypes)

	// Shuffle volumes
	random.Shuffle(len(input), func(i, j int) { input[i], input[j] = input[j], input[i] })

	// Group volumes by node
	byNode := make(map[string][]storage.VolumeMetadata)
	for _, volume := range input {
		byNode[volume.NodeID] = append(byNode[volume.NodeID], volume)
	}

	// Convert map to slice and sort node volumes by the preferred priority.
	// Volumes with the most preferred volume type are first.
	var perNode []*nodeVolumes
	for nodeID, volumes := range byNode {
		typePriority.Sort(volumes, randomSeed)
		perNode = append(perNode, &nodeVolumes{nodeID: nodeID, volumes: newStack[storage.VolumeMetadata](volumes)})
	}

	// Shuffle nodes, map order above is random, so we must sort the slice at first
	sort.Slice(perNode, func(i, j int) bool { return perNode[i].nodeID < perNode[j].nodeID })
	random.Shuffle(len(perNode), func(i, j int) { perNode[i], perNode[j] = perNode[j], perNode[i] })

	// Get up to limit volumes according preferred types
	volTypes := newStack[string](preferredTypes)
	volType, _ := volTypes.Pop()
	matchVolType := func(v storage.VolumeMetadata) bool { return volType == "" || v.Type == volType }
	for {
		found := false
		for _, node := range perNode {
			// Is limit reached?
			if len(out) == limit {
				return out
			}

			// Get top volume from the node, if any
			if vol, ok := node.volumes.PopIf(matchVolType); ok {
				found = true
				out = append(out, vol)
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
func (m priorityMap) Sort(volumes []storage.VolumeMetadata, randomSeed int64) {
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
	volumes *stack[storage.VolumeMetadata]
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
