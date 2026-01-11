package unlink

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/branchmapping"
	"github.com/keboola/keboola-as-code/internal/pkg/gitbranch"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func Command(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "unlink",
		Short: helpmsg.Read(`branch/unlink/short`),
		Long:  helpmsg.Read(`branch/unlink/long`),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			d := p.BaseScope()
			fs := d.Fs()
			logger := d.Logger()

			// Check if we're in a git repository
			if !gitbranch.IsGitRepository(ctx, fs) {
				return errors.New("not a git repository")
			}

			// Get current git branch
			currentBranch, err := gitbranch.CurrentBranch(ctx, fs)
			if err != nil {
				return err
			}

			// Check if on default branch
			if gitbranch.IsDefaultBranch(currentBranch) {
				return errors.Errorf("cannot unlink the default branch\n\n   The default branch \"%s\" is permanently linked to Keboola production.\n   This mapping cannot be removed.", currentBranch)
			}

			// Check if branch mapping file exists
			if !branchmapping.Exists(ctx, fs) {
				return errors.New("branch mapping file not found\nInitialize git-branching mode with: kbc init --git-branching")
			}

			// Load branch mappings
			mappings, err := branchmapping.Load(ctx, fs)
			if err != nil {
				return errors.Errorf("failed to load branch mappings: %w", err)
			}

			// Check if current branch is mapped
			mapping, found := mappings.GetMapping(currentBranch)
			if !found {
				logger.Infof(ctx, "Git branch \"%s\" is not linked to any Keboola branch.", currentBranch)
				return nil
			}

			// Get branch ID before removing
			branchID := mapping.GetID()
			if mapping.IsProduction() {
				branchID = "production"
			}

			// Remove the mapping
			mappings.RemoveMapping(currentBranch)

			// Save the updated mappings
			if err := branchmapping.Save(ctx, fs, mappings); err != nil {
				return errors.Errorf("failed to save branch mappings: %w", err)
			}

			logger.Infof(ctx, "Unlinked git branch \"%s\" from Keboola branch %s", currentBranch, branchID)
			logger.Info(ctx, "")
			logger.Info(ctx, "Note: The Keboola branch still exists. Delete it manually if needed:")
			logger.Infof(ctx, "  kbc remote delete branch --branch-id=%s", branchID)

			return nil
		},
	}
	return cmd
}
