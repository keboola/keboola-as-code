package create

import (
	"bufio"
	"context"
	"fmt"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/template"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/template/context/create"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
	createTemplate "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/create"
	"github.com/umisama/go-regexpcache"
)

type createTmplDialogDeps interface {
	Components() *model.ComponentsMap
	KeboolaProjectAPI() *keboola.AuthorizedAPI
	Logger() log.Logger
}

type createTmplDialog struct {
	*dialog.Dialogs
	Flags
	prompt          prompt.Prompt
	deps            createTmplDialogDeps
	selectedBranch  *model.Branch
	allConfigs      []*model.ConfigWithRows
	selectedConfigs []*model.ConfigWithRows
	out             createTemplate.Options
}

// AskCreateTemplateOpts - dialog for creating a template from an existing project.
func AskCreateTemplateOpts(ctx context.Context, d *dialog.Dialogs, deps createTmplDialogDeps, f Flags) (createTemplate.Options, error) {
	return (&createTmplDialog{
		Dialogs: d,
		prompt:  d.Prompt,
		deps:    deps,
		Flags:   f,
	}).ask(ctx)
}

func (d *createTmplDialog) ask(ctx context.Context) (createTemplate.Options, error) {
	// Get Storage API
	api := d.deps.KeboolaProjectAPI()

	// Load branches
	var allBranches []*model.Branch
	if result, err := api.ListBranchesRequest().Send(ctx); err == nil {
		for _, apiBranch := range *result {
			allBranches = append(allBranches, model.NewBranch(apiBranch))
		}
	} else {
		return d.out, err
	}

	// Name
	if d.Name.IsSet() {
		d.out.Name = d.Name.Value
	} else {
		d.out.Name = d.askName()
	}
	if err := validateTemplateName(d.out.Name); err != nil {
		return d.out, err
	}

	// ID
	if d.ID.IsSet() {
		d.out.ID = d.ID.Value
	} else {
		d.out.ID = d.askID()
	}
	if err := validateID(d.out.ID); err != nil {
		return d.out, err
	}

	// Description
	if d.Description.IsSet() {
		d.out.Description = d.Description.Value
	} else {
		d.out.Description = d.askDescription()
	}
	if err := validateTemplateDescription(d.out.Description); err != nil {
		return d.out, err
	}

	// Branch
	var err error
	d.selectedBranch, err = d.SelectBranch(allBranches, `Select the source branch`, d.Branch)
	if err != nil {
		return d.out, err
	}
	d.out.SourceBranch = d.selectedBranch.BranchKey

	// Load configs
	branchKey := keboola.BranchKey{ID: d.selectedBranch.ID}
	if result, err := api.ListConfigsAndRowsFrom(branchKey).Send(ctx); err == nil {
		for _, component := range *result {
			for _, apiConfig := range component.Configs {
				d.allConfigs = append(d.allConfigs, model.NewConfigWithRows(apiConfig))
			}
		}
	} else {
		return d.out, err
	}

	// Select configs
	if d.AllConfigs.Value {
		d.selectedConfigs = d.allConfigs
	} else {
		configs, err := d.SelectConfigs(d.allConfigs, `Select the configurations to include in the template`, d.Configs)
		if err != nil {
			return d.out, err
		}
		d.selectedConfigs = configs
	}

	// Ask for new ID for each config and row
	d.out.Configs, err = askTemplateObjectsIds(d.selectedBranch, d.selectedConfigs, d.Dialogs)
	if err != nil {
		return d.out, err
	}

	// Ask for user inputs
	objectInputs, stepsGroups, err := d.AskNewTemplateInputs(ctx, d.deps, d.selectedBranch, d.selectedConfigs, d.AllInputs)
	if err != nil {
		return d.out, err
	}
	objectInputs.SetTo(d.out.Configs)
	d.out.StepsGroups = stepsGroups

	// Ask for list of used components
	if d.UsedComponents.IsSet() {
		d.out.Components = strings.Split(d.UsedComponents.Value, `,`)
	} else {
		d.out.Components = d.askComponents(d.deps.Components().Used())
	}

	return d.out, nil
}

func (d *createTmplDialog) askComponents(all []*keboola.Component) []string {
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

func (d *createTmplDialog) askID() string {
	name, _ := d.prompt.Ask(&prompt.Question{
		Label:       `Template ID`,
		Description: "Please enter a template internal ID.\nAllowed characters: a-z, A-Z, 0-9, \"-\".\nFor example \"lorem-ipsum-ecommerce\".",
		Default:     strhelper.NormalizeName(d.out.Name),
		Validator:   validateID,
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

func validateTemplateName(val any) error {
	str := strings.TrimSpace(val.(string))
	if len(str) == 0 {
		return errors.New(`template name is required and cannot be empty`)
	}
	return nil
}

func validateTemplateDescription(val any) error {
	str := strings.TrimSpace(val.(string))
	if len(str) == 0 {
		return errors.New(`template description is required and cannot be empty`)
	}
	return nil
}

type templateIdsDialog struct {
	*dialog.Dialogs
	prompt  prompt.Prompt
	branch  *model.Branch
	configs []*model.ConfigWithRows
}

// askTemplateObjectsIds - dialog to define human-readable ID for each config and config row.
// Used in AskCreateTemplateOpts.
func askTemplateObjectsIds(branch *model.Branch, configs []*model.ConfigWithRows, d *dialog.Dialogs) ([]create.ConfigDef, error) {
	return (&templateIdsDialog{Dialogs: d, prompt: d.Prompt, branch: branch, configs: configs}).ask()
}

func (d *templateIdsDialog) ask() ([]create.ConfigDef, error) {
	result, _ := d.prompt.Editor("md", &prompt.Question{
		Description: `Please enter a human readable ID for each config and config row.`,
		Default:     d.defaultValue(),
		Validator: func(val any) error {
			if _, err := d.parse(val.(string)); err != nil {
				// Print errors to new line
				return errors.PrefixError(err, "\n")
			}
			return nil
		},
	})
	return d.parse(result)
}

func (d *templateIdsDialog) parse(result string) ([]create.ConfigDef, error) {
	idByKey := make(map[string]string)
	ids := make(map[string]bool)
	result = strhelper.StripHTMLComments(result)
	scanner := bufio.NewScanner(strings.NewReader(result))
	errs := errors.NewMultiError()
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())

		// Skip empty lines
		if len(line) == 0 {
			continue
		}

		// Parse project ID
		var key model.Key
		switch {
		case strings.HasPrefix(line, `## Config`):
			// Config ID definition
			m := regexpcache.MustCompile(` ([a-zA-Z0-9\.\-]+):([a-zA-Z0-9\.\-]+)$`).FindStringSubmatch(line)
			if m == nil {
				errs.Append(errors.Errorf(`line %d: cannot parse "%s"`, lineNum, line))
				continue
			}
			key = model.ConfigKey{BranchID: d.branch.ID, ComponentID: keboola.ComponentID(m[1]), ID: keboola.ConfigID(m[2])}
		case strings.HasPrefix(line, `### Row`):
			// Row ID definition
			m := regexpcache.MustCompile(` ([a-zA-Z0-9\.\-]+):([a-zA-Z0-9\.\-]+):([a-zA-Z0-9\.\-]+)$`).FindStringSubmatch(line)
			if m == nil {
				errs.Append(errors.Errorf(`line %d: cannot parse "%s"`, lineNum, line))
				continue
			}
			key = model.ConfigRowKey{BranchID: d.branch.ID, ComponentID: keboola.ComponentID(m[1]), ConfigID: keboola.ConfigID(m[2]), ID: keboola.RowID(m[3])}
		default:
			errs.Append(errors.Errorf(`line %d: cannot parse "%s"`, lineNum, line))
			continue
		}

		// Parse template ID
		if !scanner.Scan() {
			errs.Append(errors.New(`expected line, found EOF`))
			continue
		}
		lineNum++
		id := strings.TrimSpace(scanner.Text())
		switch {
		case len(id) == 0:
			errs.Append(errors.Errorf(`line %d: unexpected empty line`, lineNum))
			continue
		case ids[id]:
			errs.Append(errors.Errorf(`line %d: duplicate ID "%s"`, lineNum, id))
			continue
		default:
			if err := validateID(id); err != nil {
				errs.Append(errors.Errorf(`line %d: %w`, lineNum, err))
				continue
			}
			idByKey[key.String()] = id
			ids[id] = true
		}
	}

	if errs.Len() > 0 {
		return nil, errs.ErrorOrNil()
	}

	defs := make([]create.ConfigDef, 0, len(d.configs))
	for _, c := range d.configs {
		// Config definition
		id := idByKey[c.Key().String()]
		if len(id) == 0 {
			errs.Append(errors.Errorf(`missing ID for %s`, c.Desc()))
			continue
		}
		configDef := create.ConfigDef{Key: c.ConfigKey, TemplateID: id}

		for _, r := range c.Rows {
			// Row definition
			id := idByKey[r.Key().String()]
			if len(id) == 0 {
				errs.Append(errors.Errorf(`missing ID for %s`, r.Desc()))
				continue
			}
			rowDef := create.ConfigRowDef{Key: r.ConfigRowKey, TemplateID: id}
			configDef.Rows = append(configDef.Rows, rowDef)
		}

		defs = append(defs, configDef)
	}

	return defs, errs.ErrorOrNil()
}

func (d *templateIdsDialog) defaultValue() string {
	// Generate default IDs for configs and rows
	idByKey := make(map[string]string)
	ids := make(map[string]bool)
	for _, c := range d.configs {
		makeUniqueID(c, idByKey, ids)
		for _, r := range c.Rows {
			makeUniqueID(r, idByKey, ids)
		}
	}

	// File header - info for user
	fileHeader := `
<!--
Please enter a human readable ID for each configuration. For example "L0-raw-data-ex".
Allowed characters: a-z, A-Z, 0-9, "-".
These IDs will be used in the template.

Please edit each line below "## Config ..." and "### Row ...".
Do not edit lines starting with "#"!
-->


`
	// Add definition for each config and row
	var lines strings.Builder
	lines.WriteString(fileHeader)
	for _, c := range d.configs {
		lines.WriteString(fmt.Sprintf("## Config \"%s\" %s:%s\n%s\n\n", c.Name, c.ComponentID, c.ID, idByKey[c.Key().String()]))
		for _, r := range c.Rows {
			lines.WriteString(fmt.Sprintf("### Row \"%s\" %s:%s:%s\n%s\n\n", r.Name, r.ComponentID, r.ConfigID, r.ID, idByKey[r.Key().String()]))
		}
	}

	return lines.String()
}

func makeUniqueID(object model.Object, idByKey map[string]string, ids map[string]bool) {
	name := object.ObjectName()
	id := strhelper.NormalizeName(name)
	// The generated ID can be empty, e.g. if the name contains only special characters,
	if id == "" {
		id = strhelper.NormalizeName(object.Kind().Name)
	}

	// Ensure ID is unique
	suffix := 0
	for ids[id] {
		suffix++
		id = strhelper.NormalizeName(name + "-" + fmt.Sprintf(`%03d`, suffix))
	}

	ids[id] = true
	idByKey[object.Key().String()] = id
}

func validateID(val any) error {
	str := strings.TrimSpace(val.(string))
	if len(str) == 0 {
		return errors.New(`template ID is required`)
	}

	if !regexpcache.MustCompile(template.IDRegexp).MatchString(str) {
		return errors.Errorf(`invalid ID "%s", please use only a-z, A-Z, 0-9, "-" characters`, str)
	}

	return nil
}
