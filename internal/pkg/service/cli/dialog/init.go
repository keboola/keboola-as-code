package dialog

import (
	"context"

	"github.com/keboola/go-client/pkg/client"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	createManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/manifest/create"
	workflowsGen "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/workflows/generate"
	initOp "github.com/keboola/keboola-as-code/pkg/lib/operation/project/sync/init"
)

type initDeps interface {
	Options() *options.Options
	StorageAPIClient() client.Sender
}

type hostAndTokenDependencies interface {
	Logger() log.Logger
	Options() *options.Options
}

func (p *Dialogs) AskHostAndToken(d hostAndTokenDependencies) error {
	// Host and token
	errs := errors.NewMultiError()
	if _, err := p.AskStorageAPIHost(d); err != nil {
		errs.Append(err)
	}
	if _, err := p.AskStorageAPIToken(d); err != nil {
		errs.Append(err)
	}
	if errs.Len() > 0 {
		return errs
	}
	return nil
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
	options := d.Options()
	if options.IsSet("ci") {
		if options.IsSet("ci-validate") || options.IsSet("ci-push") || options.IsSet("ci-pull") {
			return out, errors.New("`ci-*` flags may not be set if `ci` is set to `false`")
		}

		out.Workflows = workflowsGen.Options{
			Validate:   options.GetBool("ci"),
			Push:       options.GetBool("ci"),
			Pull:       options.GetBool("ci"),
			MainBranch: options.GetString("ci-main-branch"),
		}
	} else {
		if p.Confirm(&prompt.Confirm{Label: "Generate workflows files for GitHub Actions?", Default: true}) {
			out.Workflows = p.AskWorkflowsOptions(options)
		}
	}

	return out, nil
}
