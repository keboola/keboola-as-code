package dialog

import (
	"github.com/keboola/keboola-as-code/internal/pkg/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/remote"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	createManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/local/manifest/create"
	initOp "github.com/keboola/keboola-as-code/pkg/lib/operation/sync/init"
)

type initDeps interface {
	Options() *options.Options
	StorageApi() (*remote.StorageApi, error)
}

func (p *Dialogs) AskInitOptions(d initDeps) (initOp.Options, error) {
	// Default values + values for non-interactive terminal
	out := initOp.Options{
		Pull:            true,
		ManifestOptions: createManifest.Options{},
	}

	o := d.Options()

	// Host and token
	errors := utils.NewMultiError()
	if _, err := p.AskStorageApiHost(o); err != nil {
		errors.Append(err)
	}
	if _, err := p.AskStorageApiToken(o); err != nil {
		errors.Append(err)
	}
	if errors.Len() > 0 {
		return out, errors
	}

	// Allowed branches
	if allowedBranches, err := p.AskAllowedBranches(d); err == nil {
		out.ManifestOptions.AllowedBranches = allowedBranches
	} else {
		return out, err
	}

	// Naming
	if p.Confirm(&prompt.Confirm{
		Label: "Do you want to include object IDs in directory structure?",
		Description: `The directory structure can optionally contain object IDs. Example:
- path with IDs:    83065-dev-branch/writer/keboola.wr-db-snowflake/734333057-power-bi/rows/734333064-orders
- path without IDs: dev-branch/writer/keboola.wr-db-snowflake/power-bi/rows/orders`,
		Default: false,
	}) {
		out.ManifestOptions.Naming = naming.TemplateWithIds()
	} else {
		out.ManifestOptions.Naming = naming.TemplateWithoutIds()
	}

	// Ask for workflows options
	if p.Confirm(&prompt.Confirm{Label: "Generate workflows files for GitHub Actions?", Default: true}) {
		out.Workflows = p.AskWorkflowsOptions(o)
	}

	return out, nil
}
