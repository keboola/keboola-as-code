package dialog

import (
	"fmt"
	"strings"

	"github.com/keboola/go-client/pkg/sandbox"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	workspace "github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/workspace"
)

type createWorkspaceDeps interface {
	Options() *options.Options
}

func (p *Dialogs) AskCreateWorkspace(d createWorkspaceDeps) (workspace.CreateOptions, error) {
	opts := workspace.CreateOptions{}

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

	size, err := p.askWorkspaceSize(d)
	if err != nil {
		return opts, err
	}
	opts.Size = size

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
			return "", fmt.Errorf("missing name, please specify it")
		}
		return name, nil
	}
}

var workspaceTypes = []string{
	"python",
	"snowflake",
	"r",
}

func (p *Dialogs) askWorkspaceType(d createWorkspaceDeps) (string, error) {
	if d.Options().IsSet("type") {
		typ := d.Options().GetString("type")
		if !utils.Contains(workspaceTypes, typ) {
			return "", fmt.Errorf("invalid workspace type, must be one of: %s", strings.Join(workspaceTypes, ", "))
		}
		return typ, nil
	} else {
		v, ok := p.Select(&prompt.Select{
			Label:   "Select a type for the new workspace",
			Options: workspaceTypes,
		})
		if !ok {
			return "", fmt.Errorf("missing workspace type, please specify it")
		}
		return v, nil
	}
}

var workspaceSizes = []string{
	sandbox.SizeSmall,
	sandbox.SizeMedium,
	sandbox.SizeLarge,
}

func (p *Dialogs) askWorkspaceSize(d createWorkspaceDeps) (string, error) {
	if d.Options().IsSet("size") {
		size := d.Options().GetString("size")
		if !utils.Contains(workspaceSizes, size) {
			return "", fmt.Errorf("invalid workspace size, must be one of: %s", strings.Join(workspaceSizes, ", "))
		}
		return size, nil
	} else {
		v, ok := p.Select(&prompt.Select{
			Label:   "Select a size for the new workspace",
			Options: workspaceSizes,
		})
		if !ok {
			return "", fmt.Errorf("missing workspace size, please specify it")
		}
		return v, nil
	}
}
