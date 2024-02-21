package branch

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	createBranch "github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/create/branch"
)

func AskCreateBranch(d *dialog.Dialogs, branchName configmap.Value[string]) (createBranch.Options, error) {
	out := createBranch.Options{Pull: true}

	// Name
	name, err := d.AskObjectName(`branch`, branchName)
	if err != nil {
		return out, err
	}
	out.Name = name

	return out, nil
}
