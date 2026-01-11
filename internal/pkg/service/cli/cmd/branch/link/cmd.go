package link

import (
	"context"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/branchmapping"
	"github.com/keboola/keboola-as-code/internal/pkg/gitbranch"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Flags struct {
	StorageAPIHost  configmap.Value[string] `configKey:"storage-api-host" configShorthand:"H" configUsage:"storage API host, eg. \"connection.keboola.com\""`
	StorageAPIToken configmap.Value[string] `configKey:"storage-api-token" configShorthand:"t" configUsage:"storage API token from your project"`
	BranchID        configmap.Value[string] `configKey:"branch-id" configUsage:"link to existing Keboola branch by ID"`
	BranchName      configmap.Value[string] `configKey:"branch-name" configUsage:"link to existing branch by name, or create with this name"`
}

func DefaultFlags() Flags {
	return Flags{}
}

func Command(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "link",
		Short: helpmsg.Read(`branch/link/short`),
		Long:  helpmsg.Read(`branch/link/long`),
		RunE: func(cmd *cobra.Command, args []string) (cmdErr error) {
			f := Flags{}
			if err := p.BaseScope().ConfigBinder().Bind(cmd.Context(), cmd.Flags(), args, &f); err != nil {
				return err
			}

			ctx := cmd.Context()
			baseScope := p.BaseScope()
			fs := baseScope.Fs()
			logger := baseScope.Logger()

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
				return errors.Errorf("cannot link the default branch\n\n   The default branch \"%s\" is automatically linked to Keboola production\n   during 'kbc init --git-branching'. This mapping cannot be changed.\n\n   Switch to a feature branch to create a new mapping:\n   git checkout -b feature/my-feature\n   kbc branch link", currentBranch)
			}

			// Load or create branch mappings
			var mappings *branchmapping.File
			if branchmapping.Exists(ctx, fs) {
				mappings, err = branchmapping.Load(ctx, fs)
				if err != nil {
					return errors.Errorf("failed to load branch mappings: %w", err)
				}
			} else {
				mappings = branchmapping.New()
			}

			// Check if already linked
			if mapping, found := mappings.GetMapping(currentBranch); found {
				if mapping.IsProduction() {
					logger.Infof(ctx, "Git branch \"%s\" is already linked to Keboola production.", currentBranch)
				} else {
					logger.Infof(ctx, "Git branch \"%s\" is already linked to Keboola branch %s (%s).", currentBranch, mapping.GetID(), mapping.Name)
				}
				return nil
			}

			// Get remote command scope for API access
			d, err := p.RemoteCommandScope(ctx, f.StorageAPIHost, f.StorageAPIToken)
			if err != nil {
				return err
			}

			// Send cmd successful/failed event
			defer d.EventSender().SendCmdEvent(ctx, d.Clock().Now(), &cmdErr, "branch-link")

			var keboolaBranch *keboola.Branch

			// Handle different linking modes
			if f.BranchID.Value != "" {
				// Link to existing branch by ID
				keboolaBranch, err = findBranchByID(ctx, d, f.BranchID.Value)
				if err != nil {
					return err
				}
			} else {
				// Determine branch name to use
				branchName := currentBranch
				if f.BranchName.Value != "" {
					branchName = f.BranchName.Value
				}

				// Try to find existing branch with this name
				keboolaBranch, err = findBranchByName(ctx, d, branchName)
				if err != nil {
					return err
				}

				// If not found, create new branch
				if keboolaBranch == nil {
					logger.Infof(ctx, "Creating Keboola branch \"%s\"...", branchName)
					keboolaBranch, err = createBranch(ctx, d, branchName)
					if err != nil {
						return errors.Errorf("failed to create Keboola branch: %w", err)
					}
					logger.Infof(ctx, "Created Keboola branch \"%s\" (ID: %d)", keboolaBranch.Name, keboolaBranch.ID)
				}
			}

			// Create the mapping
			branchIDStr := keboolaBranch.ID.String()
			mapping := &branchmapping.BranchMapping{
				ID:   &branchIDStr,
				Name: keboolaBranch.Name,
			}
			mappings.SetMapping(currentBranch, mapping)

			// Save the updated mappings
			if err := branchmapping.Save(ctx, fs, mappings); err != nil {
				return errors.Errorf("failed to save branch mappings: %w", err)
			}

			logger.Infof(ctx, "Linked git branch \"%s\" → Keboola branch %d (%s)", currentBranch, keboolaBranch.ID, keboolaBranch.Name)

			return nil
		},
	}

	configmap.MustGenerateFlags(cmd.Flags(), DefaultFlags())
	return cmd
}

func findBranchByID(ctx context.Context, d dependencies.RemoteCommandScope, branchID string) (*keboola.Branch, error) {
	branches, err := d.KeboolaProjectAPI().ListBranchesRequest().Send(ctx)
	if err != nil {
		return nil, errors.Errorf("failed to list branches: %w", err)
	}

	for _, branch := range *branches {
		if branch.ID.String() == branchID {
			return branch, nil
		}
	}

	return nil, errors.Errorf("Keboola branch with ID %s not found", branchID)
}

func findBranchByName(ctx context.Context, d dependencies.RemoteCommandScope, name string) (*keboola.Branch, error) {
	branches, err := d.KeboolaProjectAPI().ListBranchesRequest().Send(ctx)
	if err != nil {
		return nil, errors.Errorf("failed to list branches: %w", err)
	}

	for _, branch := range *branches {
		if branch.Name == name && !branch.IsDefault {
			return branch, nil
		}
	}

	return nil, nil
}

func createBranch(ctx context.Context, d dependencies.RemoteCommandScope, name string) (*keboola.Branch, error) {
	branch, err := d.KeboolaProjectAPI().CreateBranchRequest(&keboola.Branch{
		Name: name,
	}).Send(ctx)
	if err != nil {
		return nil, err
	}
	return branch, nil
}
