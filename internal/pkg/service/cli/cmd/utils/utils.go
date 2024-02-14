package utils

import (
	"fmt"
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/umisama/go-regexpcache"
	"strings"
)

func AskWorkspace(allWorkspaces []*keboola.WorkspaceWithConfig, d *dialog.Dialogs, workspaceId configmap.Value[string]) (*keboola.WorkspaceWithConfig, error) {
	if workspaceId.IsSet() {
		workspaceID := workspaceId.Value
		for _, w := range allWorkspaces {
			if string(w.Config.ID) == workspaceID {
				return w, nil
			}
		}
		return nil, errors.Errorf(`workspace with ID "%s" not found in the project`, workspaceID)
	}

	selectOpts := make([]string, 0)
	for _, w := range allWorkspaces {
		selectOpts = append(selectOpts, fmt.Sprintf(`%s (%s)`, w.Config.Name, w.Config.ID))
	}
	if index, ok := d.SelectIndex(&prompt.SelectIndex{
		Label:   "Workspace",
		Options: selectOpts,
	}); ok {
		return allWorkspaces[index], nil
	}

	return nil, errors.New(`please specify workspace`)
}

func AskTargetName(d *dialog.Dialogs, targetName configmap.Value[string]) (string, error) {
	fmt.Println(targetName.Value)
	var name string
	if targetName.IsSet() {
		name = targetName.Value
	} else {
		fmt.Println("dasdas")
		name = askTargetName(d)
	}
	if err := validateTargetName(name); err != nil {
		return "", err
	}

	return name, nil
}

func askTargetName(d *dialog.Dialogs) string {
	name, _ := d.Ask(&prompt.Question{
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
