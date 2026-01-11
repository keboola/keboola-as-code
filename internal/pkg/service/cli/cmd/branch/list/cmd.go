package list

import (
	"sort"

	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/branchmapping"
	"github.com/keboola/keboola-as-code/internal/pkg/gitbranch"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func Command(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: helpmsg.Read(`branch/list/short`),
		Long:  helpmsg.Read(`branch/list/long`),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			d := p.BaseScope()
			fs := d.Fs()
			logger := d.Logger()

			// Check if we're in a git repository
			if !gitbranch.IsGitRepository(ctx, fs) {
				return errors.New("not a git repository")
			}

			// Get current git branch for highlighting
			currentBranch, err := gitbranch.CurrentBranch(ctx, fs)
			if err != nil {
				currentBranch = ""
			}

			// Check if branch mapping file exists
			if !branchmapping.Exists(ctx, fs) {
				logger.Info(ctx, "No branch mappings found.")
				logger.Info(ctx, "Initialize git-branching mode with: kbc init --git-branching")
				return nil
			}

			// Load branch mappings
			mappings, err := branchmapping.Load(ctx, fs)
			if err != nil {
				return errors.Errorf("failed to load branch mappings: %w", err)
			}

			if len(mappings.Mappings) == 0 {
				logger.Info(ctx, "No branch mappings found.")
				return nil
			}

			// Get sorted list of git branches
			gitBranches := make([]string, 0, len(mappings.Mappings))
			for gitBranch := range mappings.Mappings {
				gitBranches = append(gitBranches, gitBranch)
			}
			sort.Strings(gitBranches)

			// Print header
			logger.Infof(ctx, "%-20s %-20s %s", "Git Branch", "Keboola Branch ID", "Keboola Branch Name")
			logger.Info(ctx, "─────────────────────────────────────────────────────────────")

			// Print mappings
			for _, gitBranch := range gitBranches {
				mapping := mappings.Mappings[gitBranch]
				var branchID string
				if mapping.IsProduction() {
					branchID = "(production)"
				} else {
					branchID = mapping.GetID()
				}

				marker := " "
				if gitBranch == currentBranch {
					marker = "*"
				}

				logger.Infof(ctx, "%s%-19s → %-18s %s", marker, gitBranch, branchID, mapping.Name)
			}

			// Show current branch indicator if it's in the list
			if currentBranch != "" {
				if _, found := mappings.GetMapping(currentBranch); found {
					logger.Info(ctx, "")
					logger.Infof(ctx, "* %s (current)", currentBranch)
				}
			}

			return nil
		},
	}
	return cmd
}
