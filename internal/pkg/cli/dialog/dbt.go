package dialog

import (
	"fmt"
	"strings"

	"github.com/umisama/go-regexpcache"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/dbt/generate/profile"
)

type dbtGenerateProfileDialogDeps interface {
	Logger() log.Logger
	Options() *options.Options
	Components() *model.ComponentsMap
}

type dbtGenerateProfileDialog struct {
	*Dialogs
	prompt prompt.Prompt
	deps   dbtGenerateProfileDialogDeps
	out    profile.Options
}

// AskDbtGenerateProfile - dialog for generating a dbt profile.
func (p *Dialogs) AskDbtGenerateProfile(deps dbtGenerateProfileDialogDeps) (profile.Options, error) {
	return (&dbtGenerateProfileDialog{
		Dialogs: p,
		prompt:  p.Prompt,
		deps:    deps,
	}).ask()
}

func (d *dbtGenerateProfileDialog) ask() (profile.Options, error) {
	// Target Name
	if d.deps.Options().IsSet(`target-name`) {
		d.out.TargetName = d.deps.Options().GetString(`target-name`)
	} else {
		d.out.TargetName = d.askTargetName()
	}
	if err := validateTargetName(d.out.TargetName); err != nil {
		return d.out, err
	}

	return d.out, nil
}

func (d *dbtGenerateProfileDialog) askTargetName() string {
	name, _ := d.prompt.Ask(&prompt.Question{
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
