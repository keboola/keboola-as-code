package cmdutil

import (
	"context"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
)

func TestPropagateContext(t *testing.T) {
	t.Parallel()
	// Create a context with a test value
	type contextKey string
	testKey := contextKey("test-key")
	testValue := "test-value"
	ctx := context.WithValue(t.Context(), testKey, testValue)

	// Create a command hierarchy
	rootCmd := &cobra.Command{Use: "root"}
	rootCmd.SetContext(ctx)

	// Add some subcommands
	level1Cmd1 := &cobra.Command{Use: "level1-1"}
	level1Cmd2 := &cobra.Command{Use: "level1-2"}
	rootCmd.AddCommand(level1Cmd1, level1Cmd2)

	// Add nested subcommands
	level2Cmd1 := &cobra.Command{Use: "level2-1"}
	level2Cmd2 := &cobra.Command{Use: "level2-2"}
	level1Cmd1.AddCommand(level2Cmd1)
	level1Cmd2.AddCommand(level2Cmd2)

	// Propagate context
	PropagateContext(rootCmd)

	// Verify that context is propagated to all subcommands
	assert.Equal(t, testValue, level1Cmd1.Context().Value(testKey))
	assert.Equal(t, testValue, level1Cmd2.Context().Value(testKey))
	assert.Equal(t, testValue, level2Cmd1.Context().Value(testKey))
	assert.Equal(t, testValue, level2Cmd2.Context().Value(testKey))
}
