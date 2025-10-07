package create

import (
	"strings"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/workspace/create"
)

// sandboxWorkspaceTypesToStrings converts a slice of SandboxWorkspaceType to []string.
// This helper function is needed because the SDK returns custom types that need to be converted to strings.
func sandboxWorkspaceTypesToStrings(types []keboola.SandboxWorkspaceType) []string {
	result := make([]string, len(types))
	for i, typ := range types {
		result[i] = string(typ)
	}
	return result
}

func AskCreateWorkspace(d *dialog.Dialogs, f Flags) (create.CreateOptions, error) {
	opts := create.CreateOptions{}

	name, err := askWorkspaceName(d, f.Name)
	if err != nil {
		return opts, err
	}
	opts.Name = name

	typ, err := askWorkspaceType(d, f.Type)
	if err != nil {
		return opts, err
	}
	opts.Type = typ

	if keboola.SandboxWorkspaceSupportsSizes(keboola.SandboxWorkspaceType(typ)) {
		size, err := askWorkspaceSize(d, f.Size)
		if err != nil {
			return opts, err
		}
		opts.Size = size
	}

	return opts, nil
}

func askWorkspaceName(d *dialog.Dialogs, workspaceName configmap.Value[string]) (string, error) {
	if workspaceName.IsSet() {
		return workspaceName.Value, nil
	} else {
		name, ok := d.Ask(&prompt.Question{
			Label:     "Enter a name for the new workspace",
			Validator: prompt.ValueRequired,
		})
		if !ok || len(name) == 0 {
			return "", errors.New("missing name, please specify it")
		}
		return name, nil
	}
}

func askWorkspaceType(d *dialog.Dialogs, workspaceType configmap.Value[string]) (string, error) {
	if workspaceType.IsSet() {
		typ := workspaceType.Value
		// Convert the string to SandboxWorkspaceType for map lookup
		if !keboola.SandboxWorkspaceTypesMap()[keboola.SandboxWorkspaceType(typ)] {
			return "", errors.Errorf("invalid workspace type, must be one of: %s", strings.Join(sandboxWorkspaceTypesToStrings(keboola.SandboxWorkspaceTypesOrdered()), ", "))
		}
		return typ, nil
	} else {
		v, ok := d.Select(&prompt.Select{
			Label:   "Select a type for the new workspace",
			Options: sandboxWorkspaceTypesToStrings(keboola.SandboxWorkspaceTypesOrdered()),
		})
		if !ok {
			return "", errors.New("missing workspace type, please specify it")
		}
		return v, nil
	}
}

func askWorkspaceSize(d *dialog.Dialogs, workspaceSize configmap.Value[string]) (string, error) {
	if workspaceSize.IsSet() {
		size := workspaceSize.Value
		if !keboola.SandboxWorkspaceSizesMap()[size] {
			return "", errors.Errorf("invalid workspace size, must be one of: %s", strings.Join(keboola.SandboxWorkspaceSizesOrdered(), ", "))
		}
		return size, nil
	} else {
		v, ok := d.Select(&prompt.Select{
			Label:   "Select a size for the new workspace",
			Options: keboola.SandboxWorkspaceSizesOrdered(),
		})
		if !ok {
			return "", errors.New("missing workspace size, please specify it")
		}
		return v, nil
	}
}
