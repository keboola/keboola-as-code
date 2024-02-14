package create

import (
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/workspace/create"
	"strings"
)

func AskCreateWorkspace(flag Flags, dialog *dialog.Dialogs) (create.CreateOptions, error) {
	opts := create.CreateOptions{}

	name, err := askWorkspaceName(flag, dialog)
	if err != nil {
		return opts, err
	}
	opts.Name = name

	typ, err := askWorkspaceType(flag, dialog)
	if err != nil {
		return opts, err
	}
	opts.Type = typ

	if keboola.WorkspaceSupportsSizes(typ) {
		size, err := askWorkspaceSize(flag, dialog)
		if err != nil {
			return opts, err
		}
		opts.Size = size
	}

	return opts, nil
}

func askWorkspaceName(f Flags, d *dialog.Dialogs) (string, error) {
	if f.Name.IsSet() {
		return f.Name.Value, nil
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

func askWorkspaceType(f Flags, d *dialog.Dialogs) (string, error) {
	if f.Type.IsSet() {
		typ := f.Type.Value
		if !keboola.WorkspaceTypesMap()[typ] {
			return "", errors.Errorf("invalid workspace type, must be one of: %s", strings.Join(keboola.WorkspaceTypesOrdered(), ", "))
		}
		return typ, nil
	} else {
		v, ok := d.Select(&prompt.Select{
			Label:   "Select a type for the new workspace",
			Options: keboola.WorkspaceTypesOrdered(),
		})
		if !ok {
			return "", errors.New("missing workspace type, please specify it")
		}
		return v, nil
	}
}

func askWorkspaceSize(f Flags, d *dialog.Dialogs) (string, error) {
	if f.Size.IsSet() {
		size := f.Size.Value
		if !keboola.WorkspaceSizesMap()[size] {
			return "", errors.Errorf("invalid workspace size, must be one of: %s", strings.Join(keboola.WorkspaceSizesOrdered(), ", "))
		}
		return size, nil
	} else {
		v, ok := d.Select(&prompt.Select{
			Label:   "Select a size for the new workspace",
			Options: keboola.WorkspaceSizesOrdered(),
		})
		if !ok {
			return "", errors.New("missing workspace size, please specify it")
		}
		return v, nil
	}
}
