package create

//
// import (
//	"context"
//	"fmt"
//	"github.com/keboola/go-client/pkg/keboola"
//	"github.com/keboola/keboola-as-code/internal/pkg/log"
//	"github.com/keboola/keboola-as-code/internal/pkg/model"
//	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog"
//	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt"
//	"github.com/keboola/keboola-as-code/internal/pkg/template"
//	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
//	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
//	createTemplate "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/create"
//	"github.com/umisama/go-regexpcache"
//	"strings"
//)
//
// type createTmplDialogDeps interface {
//	Components() *model.ComponentsMap
//	KeboolaProjectAPI() *keboola.API
//	Logger() log.Logger
//}
//
// type createTmplDialog struct {
//	*dialog.Dialogs
//	prompt          prompt.Prompt
//	deps            createTmplDialogDeps
//	selectedBranch  *model.Branch
//	allConfigs      []*model.ConfigWithRows
//	selectedConfigs []*model.ConfigWithRows
//	out             createTemplate.Options
//}
//
//// AskCreateTemplateOpts - dialog for creating a template from an existing project.
// func AskCreateTemplateOpts(ctx context.Context, d *dialog.Dialogs, deps createTmplDialogDeps, flags Flags) (createTemplate.Options, error) {
//	return (&createTmplDialog{
//		Dialogs: d,
//		prompt:  d.Prompt,
//		deps:    deps,
//	}).ask(ctx, flags)
//}
//
//func (d *createTmplDialog) ask(ctx context.Context, flags Flags) (createTemplate.Options, error) {
//	// Get Storage API
//	api := d.deps.KeboolaProjectAPI()
//
//	// Load branches
//	var allBranches []*model.Branch
//	if result, err := api.ListBranchesRequest().Send(ctx); err == nil {
//		for _, apiBranch := range *result {
//			allBranches = append(allBranches, model.NewBranch(apiBranch))
//		}
//	} else {
//		return d.out, err
//	}
//
//	// Name
//	if flags.Name.IsSet() {
//		d.out.Name = flags.Name.Value
//	} else {
//		d.out.Name = askName(d.Dialogs)
//	}
//	if err := validateTemplateName(d.out.Name); err != nil {
//		return d.out, err
//	}
//
//	// ID
//	if flags.ID.IsSet() {
//		d.out.ID = flags.ID.Value
//	} else {
//		d.out.ID = askID(d.Dialogs, flags)
//	}
//	if err := validateID(d.out.ID); err != nil {
//		return d.out, err
//	}
//
//	// Description
//	if flags.Description.IsSet() {
//		d.out.Description = flags.Description.Value
//	} else {
//		d.out.Description = askDescription(d.Dialogs)
//	}
//	if err := validateTemplateDescription(d.out.Description); err != nil {
//		return d.out, err
//	}
//
//	// Branch
//	var err error
//	d.selectedBranch, err = d.SelectBranch(allBranches, flags.Branch, `Select the source branch`)
//	if err != nil {
//		return d.out, err
//	}
//	d.out.SourceBranch = d.selectedBranch.BranchKey
//
//	// Load configs
//	branchKey := keboola.BranchKey{ID: d.selectedBranch.ID}
//	if result, err := api.ListConfigsAndRowsFrom(branchKey).Send(ctx); err == nil {
//		for _, component := range *result {
//			for _, apiConfig := range component.Configs {
//				d.allConfigs = append(d.allConfigs, model.NewConfigWithRows(apiConfig))
//			}
//		}
//	} else {
//		return d.out, err
//	}
//
//	// Select configs
//	if flags.AllConfigs.IsSet() {
//		d.selectedConfigs = d.allConfigs
//	} else {
//		configs, err := d.SelectConfigs(d.allConfigs, `Select the configurations to include in the template`)
//		if err != nil {
//			return d.out, err
//		}
//		d.selectedConfigs = configs
//	}
//
//	// Ask for new ID for each config and row
//	d.out.Configs, err = d.AskTemplateObjectsIds(d.selectedBranch, d.selectedConfigs)
//	if err != nil {
//		return d.out, err
//	}
//
//	// Ask for user inputs
//	objectInputs, stepsGroups, err := d.AskNewTemplateInputs(ctx, d.deps, d.selectedBranch, d.selectedConfigs)
//	if err != nil {
//		return d.out, err
//	}
//	objectInputs.SetTo(d.out.Configs)
//	d.out.StepsGroups = stepsGroups
//
//	// Ask for list of used components
//	if flags.UsedComponents.IsSet() {
//		d.out.Components = strings.Split(flags.UsedComponents.Value, `,`)
//	} else {
//		d.out.Components = d.askComponents(d.deps.Components().Used())
//	}
//
//	return d.out, nil
//}
//
//func (d *createTmplDialog) askComponents(all []*keboola.Component) []string {
//	opts := make([]string, 0)
//	for _, c := range all {
//		opts = append(opts, fmt.Sprintf("%s (%s)", c.Name, c.ID))
//	}
//
//	selected, _ := d.prompt.MultiSelectIndex(&prompt.MultiSelectIndex{
//		Label:       `Used Components`,
//		Description: "Select the components that are used in the templates.",
//		Options:     opts,
//	})
//
//	res := make([]string, 0)
//	for _, s := range selected {
//		res = append(res, all[s].ID.String())
//	}
//	return res
//}
//
//func askName(d *dialog.Dialogs) string {
//	name, _ := d.Prompt.Ask(&prompt.Question{
//		Label:       `Template name`,
//		Description: "Please enter a template public name for users.\nFor example \"Lorem Ipsum Ecommerce\".",
//		Validator:   validateTemplateName,
//	})
//	return strings.TrimSpace(name)
//}
//
//func askID(d *dialog.Dialogs, flag Flags) string {
//	name, _ := d.Prompt.Ask(&prompt.Question{
//		Label:       `Template ID`,
//		Description: "Please enter a template internal ID.\nAllowed characters: a-z, A-Z, 0-9, \"-\".\nFor example \"lorem-ipsum-ecommerce\".",
//		Default:     strhelper.NormalizeName(flag.Name.Value),
//		Validator:   validateID,
//	})
//	return strings.TrimSpace(name)
//}
//
//func askDescription(d *dialog.Dialogs) string {
//	result, _ := d.Prompt.Editor("txt", &prompt.Question{
//		Description: `Please enter a short template description.`,
//		Default:     `Full workflow to ...`,
//		Validator:   validateTemplateDescription,
//	})
//	return strings.TrimSpace(result)
//}
//
//func validateTemplateName(val any) error {
//	str := strings.TrimSpace(val.(string))
//	if len(str) == 0 {
//		return errors.New(`template name is required and cannot be empty`)
//	}
//	return nil
//}
//
//func validateTemplateDescription(val any) error {
//	str := strings.TrimSpace(val.(string))
//	if len(str) == 0 {
//		return errors.New(`template description is required and cannot be empty`)
//	}
//	return nil
//}
//
//func validateID(val any) error {
//	str := strings.TrimSpace(val.(string))
//	if len(str) == 0 {
//		return errors.New(`template ID is required`)
//	}
//
//	if !regexpcache.MustCompile(template.IDRegexp).MatchString(str) {
//		return errors.Errorf(`invalid ID "%s", please use only a-z, A-Z, 0-9, "-" characters`, str)
//	}
//
//	return nil
//}
