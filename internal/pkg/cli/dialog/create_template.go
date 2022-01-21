package dialog

import (
	"bufio"
	"fmt"
	"strings"

	"github.com/umisama/go-regexpcache"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/remote"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
	createTemplate "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/create"
)

type createTmplDialogDeps interface {
	Options() *options.Options
	StorageApi() (*remote.StorageApi, error)
	ProjectState(loadOptions loadState.Options) (*project.State, error)
}

type createTmplDialog struct {
	*Dialogs
	prompt          prompt.Prompt
	deps            createTmplDialogDeps
	selectedBranch  *model.Branch
	allConfigs      []*model.Config
	selectedConfigs []*model.Config
	rowsByConfigKey map[string][]*model.ConfigRow
	out             createTemplate.Options
}

func (p *Dialogs) AskCreateTemplateOpts(deps createTmplDialogDeps) (createTemplate.Options, error) {
	return (&createTmplDialog{
		Dialogs:         p,
		prompt:          p.Prompt,
		deps:            deps,
		rowsByConfigKey: make(map[string][]*model.ConfigRow),
	}).ask()
}

func (d *createTmplDialog) ask() (createTemplate.Options, error) {
	o := d.deps.Options()

	// Host and token
	errors := utils.NewMultiError()
	if _, err := d.AskStorageApiHost(o); err != nil {
		errors.Append(err)
	}
	if _, err := d.AskStorageApiToken(o); err != nil {
		errors.Append(err)
	}
	if errors.Len() > 0 {
		return d.out, errors
	}

	// Get Storage API
	storageApi, err := d.deps.StorageApi()
	if err != nil {
		return d.out, err
	}

	// Load branches
	allBranches, err := storageApi.ListBranches()
	if err != nil {
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
	if err := validateTemplateDescription(d.out.Name); err != nil {
		return d.out, err
	}

	// Branch
	d.selectedBranch, err = d.SelectBranch(d.deps.Options(), allBranches, `Select the source branch`)
	if err != nil {
		return d.out, err
	}

	// Load configs
	components, err := storageApi.ListComponents(d.selectedBranch.Id)
	if err != nil {
		return d.out, err
	}

	// Load configs from branch
	for _, component := range components {
		for _, config := range component.Configs {
			d.allConfigs = append(d.allConfigs, config.Config)
			d.rowsByConfigKey[config.Key().String()] = config.Rows
		}
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
	d.out.Configs, err = d.askObjectsIds()
	if err != nil {
		return d.out, err
	}

	return d.out, nil
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
		Default:     utils.NormalizeName(d.out.Name),
		Validator:   validateId,
	})
	return strings.TrimSpace(name)
}

func (d *createTmplDialog) askDescription() string {
	result, _ := d.prompt.Editor(&prompt.Question{
		Description: `Please enter a short template description.`,
		Default:     `Full workflow to ...`,
		Validator:   validateTemplateDescription,
	})
	return strings.TrimSpace(result)
}

func (d *createTmplDialog) askObjectsIds() ([]createTemplate.ConfigDef, error) {
	result, _ := d.prompt.Editor(&prompt.Question{
		Description: `Please enter a human readable ID for each config and config row.`,
		Default:     d.objectsIdsDefault(),
		Validator: func(val interface{}) error {
			if _, err := d.parseObjectsIds(val.(string)); err != nil {
				// Print errors to new line
				return utils.PrefixError("\n", err)
			}
			return nil
		},
	})
	return d.parseObjectsIds(result)
}

func (d *createTmplDialog) parseObjectsIds(result string) ([]createTemplate.ConfigDef, error) {
	idByKey := make(map[string]string)
	ids := make(map[string]bool)
	errors := utils.NewMultiError()
	scanner := bufio.NewScanner(strings.NewReader(result))
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())

		// Skip comment and empty lines
		if strings.HasPrefix(line, `# `) || len(line) == 0 {
			continue
		}

		// Parse project ID
		var key model.Key
		switch {
		case strings.HasPrefix(line, `### Config`):
			// Config ID definition
			m := regexpcache.MustCompile(` ([a-zA-Z0-9\.\-]+):([a-zA-Z0-9\.\-]+)$`).FindStringSubmatch(line)
			if m == nil {
				errors.Append(fmt.Errorf(`line %d: cannot parse "%s"`, lineNum, line))
				continue
			}
			key = model.ConfigKey{BranchId: d.selectedBranch.Id, ComponentId: model.ComponentId(m[1]), Id: model.ConfigId(m[2])}
		case strings.HasPrefix(line, `### Row`):
			// Row ID definition
			m := regexpcache.MustCompile(` ([a-zA-Z0-9\.\-]+):([a-zA-Z0-9\.\-]+):([a-zA-Z0-9\.\-]+)$`).FindStringSubmatch(line)
			if m == nil {
				errors.Append(fmt.Errorf(`line %d: cannot parse "%s"`, lineNum, line))
				continue
			}
			key = model.ConfigRowKey{BranchId: d.selectedBranch.Id, ComponentId: model.ComponentId(m[1]), ConfigId: model.ConfigId(m[2]), Id: model.RowId(m[3])}
		default:
			errors.Append(fmt.Errorf(`line %d: cannot parse "%s"`, lineNum, line))
			continue
		}

		// Parse template ID
		if !scanner.Scan() {
			errors.Append(fmt.Errorf(`expected line, found EOF`))
			continue
		}

		//
		lineNum++
		id := strings.TrimSpace(scanner.Text())
		switch {
		case len(id) == 0:
			errors.Append(fmt.Errorf(`line %d: unexpected empty line`, lineNum))
			continue
		case ids[id]:
			errors.Append(fmt.Errorf(`line %d: duplicate ID "%s"`, lineNum, id))
			continue
		default:
			if err := validateId(id); err != nil {
				errors.Append(fmt.Errorf(`line %d: %w`, lineNum, err))
				continue
			}
			idByKey[key.String()] = id
			ids[id] = true
		}
	}

	if errors.Len() > 0 {
		return nil, errors.ErrorOrNil()
	}

	var defs []createTemplate.ConfigDef
	for _, c := range d.selectedConfigs {
		// Config definition
		id := idByKey[c.Key().String()]
		if len(id) == 0 {
			errors.Append(fmt.Errorf(`missing ID for %s`, c.Desc()))
			continue
		}
		configDef := createTemplate.ConfigDef{Key: c.ConfigKey, TemplateId: id}

		for _, r := range d.rowsByConfigKey[c.Key().String()] {
			// Row definition
			id := idByKey[r.Key().String()]
			if len(id) == 0 {
				errors.Append(fmt.Errorf(`missing ID for %s`, r.Desc()))
				continue
			}
			rowDef := createTemplate.ConfigRowDef{Key: r.ConfigRowKey, TemplateId: id}
			configDef.Rows = append(configDef.Rows, rowDef)
		}

		defs = append(defs, configDef)
	}

	return defs, errors.ErrorOrNil()
}

func (d *createTmplDialog) objectsIdsDefault() string {
	// Generate default IDs for configs and rows
	idByKey := make(map[string]string)
	ids := make(map[string]bool)
	for _, c := range d.selectedConfigs {
		makeUniqueId(c, idByKey, ids)
		for _, r := range d.rowsByConfigKey[c.Key().String()] {
			makeUniqueId(r, idByKey, ids)
		}
	}

	// File header - info for user
	fileHeader := `
# Please enter a human readable ID for each configuration. For example "L0-raw-data-ex".
# Allowed characters: a-z, A-Z, 0-9, "-". 
# These IDs will be used in the template.

# Please edit each line below "###".
# Do not edit lines starting with "###"!


`
	// Add definition for each config and row
	var lines strings.Builder
	lines.WriteString(fileHeader)
	for _, c := range d.selectedConfigs {
		lines.WriteString(fmt.Sprintf("### Config \"%s\" %s:%s\n%s\n\n", c.Name, c.ComponentId, c.Id, idByKey[c.Key().String()]))
		for _, r := range d.rowsByConfigKey[c.Key().String()] {
			lines.WriteString(fmt.Sprintf("### Row \"%s\" %s:%s:%s\n%s\n\n", r.Name, r.ComponentId, r.ConfigId, r.Id, idByKey[r.Key().String()]))
		}
	}

	return lines.String()
}

func makeUniqueId(object model.Object, idByKey map[string]string, ids map[string]bool) {
	name := object.ObjectName()
	id := utils.NormalizeName(name)

	// Ensure ID is unique
	suffix := 0
	for ids[id] {
		suffix++
		id = utils.NormalizeName(name + "-" + fmt.Sprintf(`%03d`, suffix))
	}

	ids[id] = true
	idByKey[object.Key().String()] = id
}

func validateTemplateName(val interface{}) error {
	str := strings.TrimSpace(val.(string))
	if len(str) == 0 {
		return fmt.Errorf(`template name is required and cannot be empty`)
	}
	return nil
}

func validateTemplateDescription(val interface{}) error {
	str := strings.TrimSpace(val.(string))
	if len(str) == 0 {
		return fmt.Errorf(`template description is required and cannot be empty`)
	}
	return nil
}

func validateId(val interface{}) error {
	str := strings.TrimSpace(val.(string))
	if len(str) == 0 {
		return fmt.Errorf(`template ID is required`)
	}

	if !regexpcache.MustCompile(template.IdRegexp).MatchString(str) {
		return fmt.Errorf(`invalid ID "%s", please use only a-z, A-Z, 0-9, "-" characters`, str)
	}

	return nil
}
