package dialog

import (
	"strings"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/umisama/go-regexpcache"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/dbt/generate/env"
	initOp "github.com/keboola/keboola-as-code/pkg/lib/operation/dbt/init"
)

func (p *Dialogs) AskTargetName() (string, error) {
	var name string
	if p.options.IsSet(`target-name`) {
		name = p.options.GetString(`target-name`)
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

func validateTargetName(val any) error {
	str := strings.TrimSpace(val.(string))
	if len(str) == 0 {
		return errors.New(`target name is required`)
	}

	if !regexpcache.MustCompile(`^[a-zA-Z0-9\_]+$`).MatchString(str) {
		return errors.Errorf(`invalid target name "%s", please use only a-z, A-Z, 0-9, "_" characters`, str)
	}

	return nil
}

func (p *Dialogs) AskGenerateEnv(allWorkspaces []*keboola.WorkspaceWithConfig) (env.Options, error) {
	targetName, err := p.AskTargetName()
	if err != nil {
		return env.Options{}, err
	}

	workspace, err := p.AskWorkspace(allWorkspaces)
	if err != nil {
		return env.Options{}, err
	}

	return env.Options{
		TargetName: targetName,
		Workspace:  workspace.Workspace,
	}, nil
}

func (p *Dialogs) AskDbtInit() (initOp.DbtInitOptions, error) {
	targetName, err := p.AskTargetName()
	if err != nil {
		return initOp.DbtInitOptions{}, err
	}

	workspaceName, err := p.askWorkspaceNameForDbtInit()
	if err != nil {
		return initOp.DbtInitOptions{}, err
	}

	return initOp.DbtInitOptions{
		TargetName:    targetName,
		WorkspaceName: workspaceName,
	}, nil
}

func (p *Dialogs) askWorkspaceNameForDbtInit() (string, error) {
	if p.options.IsSet("workspace-name") {
		return p.options.GetString("workspace-name"), nil
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
