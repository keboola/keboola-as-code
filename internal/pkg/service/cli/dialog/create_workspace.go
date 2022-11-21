package dialog

import (
	"strings"

	"github.com/keboola/go-client/pkg/sandboxesapi"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/workspace/create"
)

type createWorkspaceDeps interface {
	Options() *options.Options
}

func (p *Dialogs) AskCreateWorkspace(d createWorkspaceDeps) (create.CreateOptions, error) {
	opts := create.CreateOptions{}

	name, err := p.askWorkspaceName(d)
	if err != nil {
		return opts, err
	}
	opts.Name = name

	typ, err := p.askWorkspaceType(d)
	if err != nil {
		return opts, err
	}
	opts.Type = typ

	if sandboxesapi.SupportsSizes(typ) {
		size, err := p.askWorkspaceSize(d)
		if err != nil {
			return opts, err
		}
		opts.Size = size
	}

	return opts, nil
}

func (p *Dialogs) askWorkspaceName(d createWorkspaceDeps) (string, error) {
	if d.Options().IsSet("name") {
		return d.Options().GetString("name"), nil
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

func (p *Dialogs) askWorkspaceType(d createWorkspaceDeps) (string, error) {
	if d.Options().IsSet("type") {
		typ := d.Options().GetString("type")
		if !sandboxesapi.TypesMap()[typ] {
			return "", errors.Errorf("invalid workspace type, must be one of: %s", strings.Join(sandboxesapi.TypesOrdered(), ", "))
		}
		return typ, nil
	} else {
		v, ok := p.Select(&prompt.Select{
			Label:   "Select a type for the new workspace",
			Options: sandboxesapi.TypesOrdered(),
		})
		if !ok {
			return "", errors.New("missing workspace type, please specify it")
		}
		return v, nil
	}
}

func (p *Dialogs) askWorkspaceSize(d createWorkspaceDeps) (string, error) {
	if d.Options().IsSet("size") {
		size := d.Options().GetString("size")
		if !sandboxesapi.SizesMap()[size] {
			return "", errors.Errorf("invalid workspace size, must be one of: %s", strings.Join(sandboxesapi.SizesOrdered(), ", "))
		}
		return size, nil
	} else {
		v, ok := p.Select(&prompt.Select{
			Label:   "Select a size for the new workspace",
			Options: sandboxesapi.SizesOrdered(),
		})
		if !ok {
			return "", errors.New("missing workspace size, please specify it")
		}
		return v, nil
	}
}
