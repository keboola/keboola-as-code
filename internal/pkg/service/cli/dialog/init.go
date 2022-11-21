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
	initOp "github.com/keboola/keboola-as-code/pkg/lib/operation/project/sync/init"
)

type initDeps interface {
	Options() *options.Options
	StorageApiClient() client.Sender
}

type hostAndTokenDependencies interface {
	Logger() log.Logger
	Options() *options.Options
}

func (p *Dialogs) AskHostAndToken(d hostAndTokenDependencies) error {
	// Host and token
	errs := errors.NewMultiError()
	if _, err := p.AskStorageApiHost(d); err != nil {
		errs.Append(err)
	}
	if _, err := p.AskStorageApiToken(d); err != nil {
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
	if p.Confirm(&prompt.Confirm{Label: "Generate workflows files for GitHub Actions?", Default: true}) {
		out.Workflows = p.AskWorkflowsOptions(d.Options())
	}

	return out, nil
}
