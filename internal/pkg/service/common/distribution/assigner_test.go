package distribution_test

import (
	"fmt"
	"testing"

	"github.com/lafikl/consistent"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConsistentHashLib tests the library behavior and shows how it should be used.
func TestConsistentHashLib(t *testing.T) {
	t.Parallel()
	c := consistent.New()

	// Test no node
	_, err := c.Get("foo")
	require.Error(t, err)
	assert.Equal(t, consistent.ErrNoHosts, err)

	// Add nodes
	c.Add("node1")
	c.Add("node2")
	c.Add("node3")
	c.Add("node4")
	c.Add("node5")

	// Check distribution of the keys in 5 nodes
	keysPerNode := make(map[string]int)
	for i := 1; i <= 100; i++ {
		node, err := c.Get(fmt.Sprintf("foo%02d", i))
		require.NoError(t, err)
		keysPerNode[node]++
	}
	assert.Equal(t, map[string]int{
		"node1": 27,
		"node2": 26,
		"node3": 13,
		"node4": 24,
		"node5": 10,
	}, keysPerNode)

	// Delete nodes
	c.Remove("node2")
	c.Remove("node4")

	// Check distribution of the keys in 3 nodes
	keysPerNode = make(map[string]int)
	for i := 1; i <= 100; i++ {
		node, err := c.Get(fmt.Sprintf("foo%02d", i))
		require.NoError(t, err)
		keysPerNode[node]++
	}
	assert.Equal(t, map[string]int{
		"node1": 47,
		"node3": 30,
		"node5": 23,
	}, keysPerNode)
}
