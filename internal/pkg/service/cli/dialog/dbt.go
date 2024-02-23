package dialog

import (
	"strings"

	"github.com/umisama/go-regexpcache"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (p *Dialogs) AskTargetName(targetName configmap.Value[string]) (string, error) {
	var name string
	if targetName.IsSet() {
		name = targetName.Value
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
