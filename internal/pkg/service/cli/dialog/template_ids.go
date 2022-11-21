package dialog

import (
	"bufio"
	"fmt"
	"strings"

	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/umisama/go-regexpcache"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/template/context/create"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

type templateIdsDialog struct {
	prompt  prompt.Prompt
	branch  *model.Branch
	configs []*model.ConfigWithRows
}

// askTemplateObjectsIds - dialog to define human-readable ID for each config and config row.
// Used in AskCreateTemplateOpts.
func (p *Dialogs) askTemplateObjectsIds(branch *model.Branch, configs []*model.ConfigWithRows) ([]create.ConfigDef, error) {
	return (&templateIdsDialog{prompt: p.Prompt, branch: branch, configs: configs}).ask()
}

func (d *templateIdsDialog) ask() ([]create.ConfigDef, error) {
	result, _ := d.prompt.Editor("md", &prompt.Question{
		Description: `Please enter a human readable ID for each config and config row.`,
		Default:     d.defaultValue(),
		Validator: func(val interface{}) error {
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
	result = strhelper.StripHtmlComments(result)
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
			key = model.ConfigKey{BranchId: d.branch.Id, ComponentId: storageapi.ComponentID(m[1]), Id: storageapi.ConfigID(m[2])}
		case strings.HasPrefix(line, `### Row`):
			// Row ID definition
			m := regexpcache.MustCompile(` ([a-zA-Z0-9\.\-]+):([a-zA-Z0-9\.\-]+):([a-zA-Z0-9\.\-]+)$`).FindStringSubmatch(line)
			if m == nil {
				errs.Append(errors.Errorf(`line %d: cannot parse "%s"`, lineNum, line))
				continue
			}
			key = model.ConfigRowKey{BranchId: d.branch.Id, ComponentId: storageapi.ComponentID(m[1]), ConfigId: storageapi.ConfigID(m[2]), Id: storageapi.RowID(m[3])}
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
			if err := validateId(id); err != nil {
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

	var defs []create.ConfigDef
	for _, c := range d.configs {
		// Config definition
		id := idByKey[c.Key().String()]
		if len(id) == 0 {
			errs.Append(errors.Errorf(`missing ID for %s`, c.Desc()))
			continue
		}
		configDef := create.ConfigDef{Key: c.ConfigKey, TemplateId: id}

		for _, r := range c.Rows {
			// Row definition
			id := idByKey[r.Key().String()]
			if len(id) == 0 {
				errs.Append(errors.Errorf(`missing ID for %s`, r.Desc()))
				continue
			}
			rowDef := create.ConfigRowDef{Key: r.ConfigRowKey, TemplateId: id}
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
		makeUniqueId(c, idByKey, ids)
		for _, r := range c.Rows {
			makeUniqueId(r, idByKey, ids)
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
		lines.WriteString(fmt.Sprintf("## Config \"%s\" %s:%s\n%s\n\n", c.Name, c.ComponentId, c.Id, idByKey[c.Key().String()]))
		for _, r := range c.Rows {
			lines.WriteString(fmt.Sprintf("### Row \"%s\" %s:%s:%s\n%s\n\n", r.Name, r.ComponentId, r.ConfigId, r.Id, idByKey[r.Key().String()]))
		}
	}

	return lines.String()
}

func makeUniqueId(object model.Object, idByKey map[string]string, ids map[string]bool) {
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
