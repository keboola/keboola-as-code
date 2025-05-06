package cmdutil

import (
	"github.com/spf13/cobra"
)

// PropagateContext ensures that the context is propagated to all subcommands.
// This function should be called in the PersistentPreRunE of the root command.
func PropagateContext(cmd *cobra.Command) {
	// Set context for all subcommands
	for _, subCmd := range cmd.Commands() {
		subCmd.SetContext(cmd.Context())
		// Recursively set context for all subcommands
		PropagateContext(subCmd)
	}
}
