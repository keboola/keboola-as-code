package init

import (
	"context"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog"
	initOp "github.com/keboola/keboola-as-code/pkg/lib/operation/llm/init"
	createManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/manifest/create"
)

type initDeps interface {
	dialog.BranchesDialogDeps
	KeboolaProjectAPI() *keboola.AuthorizedAPI
}

func AskInitOptions(ctx context.Context, d *dialog.Dialogs, dep initDeps, f Flags) (initOp.Options, error) {
	// Default values + values for non-interactive terminal
	out := initOp.Options{
		ManifestOptions: createManifest.Options{
			Naming:         naming.TemplateWithoutIds(),
			AllowTargetENV: f.AllowTargetENV.Value,
		},
	}

	// Allowed branches
	if allowedBranches, err := d.AskAllowedBranches(ctx, dep, f.Branches, f.AllowTargetENV); err == nil {
		out.ManifestOptions.AllowedBranches = allowedBranches
	} else {
		return out, err
	}

	return out, nil
}
