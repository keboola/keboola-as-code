package dialog

import (
	"strings"

	"github.com/keboola/go-client/pkg/sandboxesapi"
	"github.com/umisama/go-regexpcache"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/dbt/generate/env"
	initOp "github.com/keboola/keboola-as-code/pkg/lib/operation/dbt/init"
)

type targetNameDialogDeps interface {
	Options() *options.Options
}

func (p *Dialogs) AskTargetName(d targetNameDialogDeps) (string, error) {
	var name string
	if d.Options().IsSet(`target-name`) {
		name = d.Options().GetString(`target-name`)
	} else {
		name = p.askTargetName()
	}
	if err := validateTargetName(name); err != nil {
		return "", err
	}

	return name, nil
}

func (p *Dialogs) askTargetName() string {
	name, _ := p.Ask(&prompt.Question{
		Label:       `Target Name`,
		Description: "Please enter target name.\nAllowed characters: a-z, A-Z, 0-9, \"_\".",
		Validator:   validateTargetName,
		Default:     "dev",
	})
	return strings.TrimSpace(name)
}

func validateTargetName(val interface{}) error {
	str := strings.TrimSpace(val.(string))
	if len(str) == 0 {
		return errors.New(`target name is required`)
	}

	if !regexpcache.MustCompile(`^[a-zA-Z0-9\_]+$`).MatchString(str) {
		return errors.Errorf(`invalid target name "%s", please use only a-z, A-Z, 0-9, "_" characters`, str)
	}

	return nil
}

func (p *Dialogs) AskGenerateEnv(d targetNameDialogDeps, allWorkspaces []*sandboxesapi.SandboxWithConfig) (env.GenerateEnvOptions, error) {
	targetName, err := p.AskTargetName(d)
	if err != nil {
		return env.GenerateEnvOptions{}, err
	}

	workspace, err := p.AskWorkspace(d.Options(), allWorkspaces)
	if err != nil {
		return env.GenerateEnvOptions{}, err
	}

	return env.GenerateEnvOptions{
		TargetName: targetName,
		Workspace:  workspace.Sandbox,
	}, nil
}

func (p *Dialogs) AskDbtInit(d targetNameDialogDeps) (initOp.DbtInitOptions, error) {
	targetName, err := p.AskTargetName(d)
	if err != nil {
		return initOp.DbtInitOptions{}, err
	}

	workspaceName, err := p.askWorkspaceNameForDbtInit(d)
	if err != nil {
		return initOp.DbtInitOptions{}, err
	}

	return initOp.DbtInitOptions{
		TargetName:    targetName,
		WorkspaceName: workspaceName,
	}, nil
}

func (p *Dialogs) askWorkspaceNameForDbtInit(d createWorkspaceDeps) (string, error) {
	if d.Options().IsSet("workspace-name") {
		return d.Options().GetString("workspace-name"), nil
	} else {
		name, ok := p.Ask(&prompt.Question{
			Label:     "Enter a name for a workspace to create",
			Validator: prompt.ValueRequired,
		})
		if !ok || len(name) == 0 {
			return "", errors.New("missing workspace name, please specify it")
		}
		return name, nil
	}
}
