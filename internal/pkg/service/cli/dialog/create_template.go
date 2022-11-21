package dialog

import (
	"context"
	"fmt"
	"strings"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/umisama/go-regexpcache"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
	createTemplate "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/create"
)

type createTmplDialogDeps interface {
	Logger() log.Logger
	Options() *options.Options
	Components() *model.ComponentsMap
	StorageApiClient() client.Sender
}

type createTmplDialog struct {
	*Dialogs
	prompt          prompt.Prompt
	deps            createTmplDialogDeps
	selectedBranch  *model.Branch
	allConfigs      []*model.ConfigWithRows
	selectedConfigs []*model.ConfigWithRows
	out             createTemplate.Options
}

// AskCreateTemplateOpts - dialog for creating a template from an existing project.
func (p *Dialogs) AskCreateTemplateOpts(ctx context.Context, deps createTmplDialogDeps) (createTemplate.Options, error) {
	return (&createTmplDialog{
		Dialogs: p,
		prompt:  p.Prompt,
		deps:    deps,
	}).ask(ctx)
}

func (d *createTmplDialog) ask(ctx context.Context) (createTemplate.Options, error) {
	// Host and token
	errs := errors.NewMultiError()
	if _, err := d.AskStorageApiHost(d.deps); err != nil {
		errs.Append(err)
	}
	if _, err := d.AskStorageApiToken(d.deps); err != nil {
		errs.Append(err)
	}
	if errs.Len() > 0 {
		return d.out, errs
	}

	// Get Storage API
	storageApiClient := d.deps.StorageApiClient()

	// Load branches
	var allBranches []*model.Branch
	if result, err := storageapi.ListBranchesRequest().Send(ctx, storageApiClient); err == nil {
		for _, apiBranch := range *result {
			allBranches = append(allBranches, model.NewBranch(apiBranch))
		}
	} else {
		return d.out, err
	}

	// Name
	if d.deps.Options().IsSet(`name`) {
		d.out.Name = d.deps.Options().GetString(`name`)
	} else {
		d.out.Name = d.askName()
	}
	if err := validateTemplateName(d.out.Name); err != nil {
		return d.out, err
	}

	// Id
	if d.deps.Options().IsSet(`id`) {
		d.out.Id = d.deps.Options().GetString(`id`)
	} else {
		d.out.Id = d.askId()
	}
	if err := validateId(d.out.Id); err != nil {
		return d.out, err
	}

	// Description
	if d.deps.Options().IsSet(`description`) {
		d.out.Description = d.deps.Options().GetString(`description`)
	} else {
		d.out.Description = d.askDescription()
	}
	if err := validateTemplateDescription(d.out.Description); err != nil {
		return d.out, err
	}

	// Branch
	var err error
	d.selectedBranch, err = d.SelectBranch(d.deps.Options(), allBranches, `Select the source branch`)
	if err != nil {
		return d.out, err
	}
	d.out.SourceBranch = d.selectedBranch.BranchKey

	// Load configs
	branchKey := storageapi.BranchKey{ID: d.selectedBranch.Id}
	if result, err := storageapi.ListConfigsAndRowsFrom(branchKey).Send(ctx, storageApiClient); err == nil {
		for _, component := range *result {
			for _, apiConfig := range component.Configs {
				d.allConfigs = append(d.allConfigs, model.NewConfigWithRows(apiConfig))
			}
		}
	} else {
		return d.out, err
	}

	// Select configs
	if d.deps.Options().GetBool(`all-configs`) {
		d.selectedConfigs = d.allConfigs
	} else {
		configs, err := d.SelectConfigs(d.deps.Options(), d.allConfigs, `Select the configurations to include in the template`)
		if err != nil {
			return d.out, err
		}
		d.selectedConfigs = configs
	}

	// Ask for new ID for each config and row
	d.out.Configs, err = d.askTemplateObjectsIds(d.selectedBranch, d.selectedConfigs)
	if err != nil {
		return d.out, err
	}

	// Ask for user inputs
	objectInputs, stepsGroups, err := d.askNewTemplateInputs(d.deps, d.selectedBranch, d.selectedConfigs)
	if err != nil {
		return d.out, err
	}
	objectInputs.setTo(d.out.Configs)
	d.out.StepsGroups = stepsGroups

	// Ask for list of used components
	if d.deps.Options().IsSet(`used-components`) {
		d.out.Components = strings.Split(d.deps.Options().GetString(`used-components`), `,`)
	} else {
		d.out.Components = d.askComponents(d.deps.Components().Used())
	}

	return d.out, nil
}

func (d *createTmplDialog) askComponents(all []*storageapi.Component) []string {
	opts := make([]string, 0)
	for _, c := range all {
		opts = append(opts, fmt.Sprintf("%s (%s)", c.Name, c.ID))
	}

	selected, _ := d.prompt.MultiSelectIndex(&prompt.MultiSelectIndex{
		Label:       `Used Components`,
		Description: "Select the components that are used in the templates.",
		Options:     opts,
	})

	res := make([]string, 0)
	for _, s := range selected {
		res = append(res, all[s].ID.String())
	}
	return res
}

func (d *createTmplDialog) askName() string {
	name, _ := d.prompt.Ask(&prompt.Question{
		Label:       `Template name`,
		Description: "Please enter a template public name for users.\nFor example \"Lorem Ipsum Ecommerce\".",
		Validator:   validateTemplateName,
	})
	return strings.TrimSpace(name)
}

func (d *createTmplDialog) askId() string {
	name, _ := d.prompt.Ask(&prompt.Question{
		Label:       `Template ID`,
		Description: "Please enter a template internal ID.\nAllowed characters: a-z, A-Z, 0-9, \"-\".\nFor example \"lorem-ipsum-ecommerce\".",
		Default:     strhelper.NormalizeName(d.out.Name),
		Validator:   validateId,
	})
	return strings.TrimSpace(name)
}

func (d *createTmplDialog) askDescription() string {
	result, _ := d.prompt.Editor("txt", &prompt.Question{
		Description: `Please enter a short template description.`,
		Default:     `Full workflow to ...`,
		Validator:   validateTemplateDescription,
	})
	return strings.TrimSpace(result)
}

func validateTemplateName(val interface{}) error {
	str := strings.TrimSpace(val.(string))
	if len(str) == 0 {
		return errors.New(`template name is required and cannot be empty`)
	}
	return nil
}

func validateTemplateDescription(val interface{}) error {
	str := strings.TrimSpace(val.(string))
	if len(str) == 0 {
		return errors.New(`template description is required and cannot be empty`)
	}
	return nil
}

func validateId(val interface{}) error {
	str := strings.TrimSpace(val.(string))
	if len(str) == 0 {
		return errors.New(`template ID is required`)
	}

	if !regexpcache.MustCompile(template.IdRegexp).MatchString(str) {
		return errors.Errorf(`invalid ID "%s", please use only a-z, A-Z, 0-9, "-" characters`, str)
	}

	return nil
}
