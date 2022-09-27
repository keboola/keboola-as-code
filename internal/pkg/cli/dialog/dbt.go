package dialog

import (
	"fmt"
	"strings"

	"github.com/keboola/go-client/pkg/sandboxesapi"
	"github.com/umisama/go-regexpcache"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/prompt"
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
		return fmt.Errorf(`target name is required`)
	}

	if !regexpcache.MustCompile(`^[a-zA-Z0-9\_]+$`).MatchString(str) {
		return fmt.Errorf(`invalid target name "%s", please use only a-z, A-Z, 0-9, "_" characters`, str)
	}

	return nil
}

func (p *Dialogs) AskGenerateEnv(d targetNameDialogDeps, allWorkspaces []*sandboxesapi.Sandbox) (env.GenerateEnvOptions, error) {
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
		Workspace:  workspace,
	}, nil
}
