package init

import (
	"context"

	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/ci/workflow"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	createManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/manifest/create"
	workflowsGen "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/workflows/generate"
	initOp "github.com/keboola/keboola-as-code/pkg/lib/operation/project/sync/init"
)

type initDeps interface {
	KeboolaProjectAPI() *keboola.AuthorizedAPI
}

func AskInitOptions(ctx context.Context, d *dialog.Dialogs, dep initDeps, f Flags) (initOp.Options, error) {
	// Default values + values for non-interactive terminal
	out := initOp.Options{
		Pull: true,
		ManifestOptions: createManifest.Options{
			Naming: naming.TemplateWithoutIds(),
		},
	}

	// Allowed branches
	if allowedBranches, err := d.AskAllowedBranches(ctx, dep); err == nil {
		out.ManifestOptions.AllowedBranches = allowedBranches
	} else {
		return out, err
	}

	// Ask for workflows options
	if f.CI.IsSet() {
		if f.CIValidate.IsSet() || f.CIPush.IsSet() || f.CIPull.IsSet() {
			return out, errors.New("`ci-*` flags may not be set if `ci` is set to `false`")
		}

		out.Workflows = workflowsGen.Options{
			Validate:   f.CI.Value,
			Push:       f.CI.Value,
			Pull:       f.CI.Value,
			MainBranch: f.CIMainBranch.Value,
		}
	} else if d.Confirm(&prompt.Confirm{Label: "Generate workflows files for GitHub Actions?", Default: true}) {
		out.Workflows = workflow.AskWorkflowsOptions(workflow.Flags{
			CI:           f.CI,
			CIPush:       f.CIPush,
			CIPull:       f.CIPull,
			CIMainBranch: f.CIMainBranch,
			CIValidate:   f.CIValidate,
		}, d)
	}

	return out, nil
}
