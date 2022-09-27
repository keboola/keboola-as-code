package dialog

import (
	"fmt"
	"strings"

	"github.com/keboola/go-client/pkg/sandboxesapi"
	"github.com/umisama/go-regexpcache"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/prompt"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/dbt/generate/env"
)

type targetNameDialogDeps interface {
	Options() *options.Options
}

type TargetNameOptions struct {
	Name string
}

func (p *Dialogs) AskTargetName(d targetNameDialogDeps) (TargetNameOptions, error) {
	opts := TargetNameOptions{}
	if d.Options().IsSet(`target-name`) {
		opts.Name = d.Options().GetString(`target-name`)
	} else {
		opts.Name = p.askTargetName()
	}
	if err := validateTargetName(opts.Name); err != nil {
		return opts, err
	}

	return opts, nil
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
	opts, err := p.AskTargetName(d)
	if err != nil {
		return env.GenerateEnvOptions{}, err
	}

	workspace, err := p.AskWorkspace(d.Options(), allWorkspaces)
	if err != nil {
		return env.GenerateEnvOptions{}, err
	}

	return env.GenerateEnvOptions{
		TargetName: opts.Name,
		Workspace:  workspace,
	}, nil
}
