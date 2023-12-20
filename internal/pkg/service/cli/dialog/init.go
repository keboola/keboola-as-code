package dialog

import (
	"context"

	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	createManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/manifest/create"
	workflowsGen "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/workflows/generate"
	initOp "github.com/keboola/keboola-as-code/pkg/lib/operation/project/sync/init"
)

type initDeps interface {
	KeboolaProjectAPI() *keboola.API
}

func (p *Dialogs) AskInitOptions(ctx context.Context, d initDeps) (initOp.Options, error) {
	// Default values + values for non-interactive terminal
	out := initOp.Options{
		Pull: true,
		ManifestOptions: createManifest.Options{
			Naming: naming.TemplateWithoutIds(),
		},
	}

	// Allowed branches
	if allowedBranches, err := p.AskAllowedBranches(ctx, d); err == nil {
		out.ManifestOptions.AllowedBranches = allowedBranches
	} else {
		return out, err
	}

	// Ask for workflows options
	if p.options.IsSet("ci") {
		if p.options.IsSet("ci-validate") || p.options.IsSet("ci-push") || p.options.IsSet("ci-pull") {
			return out, errors.New("`ci-*` flags may not be set if `ci` is set to `false`")
		}

		out.Workflows = workflowsGen.Options{
			Validate:   p.options.GetBool("ci"),
			Push:       p.options.GetBool("ci"),
			Pull:       p.options.GetBool("ci"),
			MainBranch: p.options.GetString("ci-main-branch"),
		}
	} else if p.Confirm(&prompt.Confirm{Label: "Generate workflows files for GitHub Actions?", Default: true}) {
		out.Workflows = p.AskWorkflowsOptions()
	}

	return out, nil
}
