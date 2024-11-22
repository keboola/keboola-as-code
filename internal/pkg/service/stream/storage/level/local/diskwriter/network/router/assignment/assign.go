// Package assignment provides proportionally assignment of the opened sink slices to source nodes.
package assignment

import (
	"math"
	"slices"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
)

func AssignSlices(all []model.SliceKey, nodes []string, nodeID string, minSlicesPerNode int) (assigned []model.SliceKey) {
	// Calculate the average number of slices per source node of the same type.
	// The value is rounded up.
	slicesCount := len(all)
	avgSlicesPerNode := int(math.Ceil(float64(slicesCount) / float64(len(nodes))))
	slicesPerNode := max(avgSlicesPerNode, minSlicesPerNode)

	// Get node index in all nodes
	index, _ := slices.BinarySearch(nodes, nodeID)

	// Proportionally assign slices to the node
	targetCount := min(slicesPerNode, slicesCount)
	assigned = make([]model.SliceKey, targetCount)
	start := index * slicesPerNode
	for i := range targetCount {
		assigned[i] = all[(start+i)%slicesCount]
	}

	return assigned
}
