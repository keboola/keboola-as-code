package dialog

import (
	"strings"

	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/workspace/create"
)

func (p *Dialogs) AskCreateWorkspace() (create.CreateOptions, error) {
	opts := create.CreateOptions{}

	name, err := p.askWorkspaceName()
	if err != nil {
		return opts, err
	}
	opts.Name = name

	typ, err := p.askWorkspaceType()
	if err != nil {
		return opts, err
	}
	opts.Type = typ

	if keboola.WorkspaceSupportsSizes(typ) {
		size, err := p.askWorkspaceSize()
		if err != nil {
			return opts, err
		}
		opts.Size = size
	}

	return opts, nil
}

func (p *Dialogs) askWorkspaceName() (string, error) {
	if p.options.IsSet("name") {
		return p.options.GetString("name"), nil
	} else {
		name, ok := p.Ask(&prompt.Question{
			Label:     "Enter a name for the new workspace",
			Validator: prompt.ValueRequired,
		})
		if !ok || len(name) == 0 {
			return "", errors.New("missing name, please specify it")
		}
		return name, nil
	}
}

func (p *Dialogs) askWorkspaceType() (string, error) {
	if p.options.IsSet("type") {
		typ := p.options.GetString("type")
		if !keboola.WorkspaceTypesMap()[typ] {
			return "", errors.Errorf("invalid workspace type, must be one of: %s", strings.Join(keboola.WorkspaceTypesOrdered(), ", "))
		}
		return typ, nil
	} else {
		v, ok := p.Select(&prompt.Select{
			Label:   "Select a type for the new workspace",
			Options: keboola.WorkspaceTypesOrdered(),
		})
		if !ok {
			return "", errors.New("missing workspace type, please specify it")
		}
		return v, nil
	}
}

func (p *Dialogs) askWorkspaceSize() (string, error) {
	if p.options.IsSet("size") {
		size := p.options.GetString("size")
		if !keboola.WorkspaceSizesMap()[size] {
			return "", errors.Errorf("invalid workspace size, must be one of: %s", strings.Join(keboola.WorkspaceSizesOrdered(), ", "))
		}
		return size, nil
	} else {
		v, ok := p.Select(&prompt.Select{
			Label:   "Select a size for the new workspace",
			Options: keboola.WorkspaceSizesOrdered(),
		})
		if !ok {
			return "", errors.New("missing workspace size, please specify it")
		}
		return v, nil
	}
}
