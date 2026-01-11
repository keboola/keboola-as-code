package status

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
		Use:   "status",
		Short: helpmsg.Read(`branch/status/short`),
		Long:  helpmsg.Read(`branch/status/long`),
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

			// Check if branch mapping file exists
			if !branchmapping.Exists(ctx, fs) {
				logger.Infof(ctx, "Git branch:      %s", currentBranch)
				logger.Infof(ctx, "Keboola branch:  (none)")
				logger.Infof(ctx, "Status:          Not linked")
				logger.Info(ctx, "")
				logger.Info(ctx, "Branch mapping file not found.")
				logger.Info(ctx, "Initialize git-branching mode with: kbc init --git-branching")
				return nil
			}

			// Load branch mappings
			mappings, err := branchmapping.Load(ctx, fs)
			if err != nil {
				return errors.Errorf("failed to load branch mappings: %w", err)
			}

			// Check if current branch is mapped
			mapping, found := mappings.GetMapping(currentBranch)

			logger.Infof(ctx, "Git branch:      %s", currentBranch)

			if found {
				if mapping.IsProduction() {
					logger.Infof(ctx, "Keboola branch:  (production) %s", mapping.Name)
				} else {
					logger.Infof(ctx, "Keboola branch:  %s (%s)", mapping.GetID(), mapping.Name)
				}
				logger.Infof(ctx, "Status:          Linked")
				logger.Info(ctx, "")
				logger.Info(ctx, "Ready to use sync commands (push, pull, diff).")
			} else {
				logger.Infof(ctx, "Keboola branch:  (none)")
				logger.Infof(ctx, "Status:          Not linked")
				logger.Info(ctx, "")
				logger.Info(ctx, "Run 'kbc branch link' to create and link a Keboola branch.")
			}

			return nil
		},
	}
	return cmd
}
