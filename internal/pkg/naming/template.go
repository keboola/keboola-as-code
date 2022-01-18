package naming

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/umisama/go-regexpcache"
)

// Template of the files names.
type Template struct {
	Branch              PathTemplate `json:"branch" validate:"required_in_project"`
	Config              PathTemplate `json:"config" validate:"required"`
	ConfigRow           PathTemplate `json:"configRow" validate:"required"`
	SchedulerConfig     PathTemplate `json:"schedulerConfig" validate:"required"`
	SharedCodeConfig    PathTemplate `json:"sharedCodeConfig" validate:"required"`
	SharedCodeConfigRow PathTemplate `json:"sharedCodeConfigRow" validate:"required"`
	VariablesConfig     PathTemplate `json:"variablesConfig" validate:"required"`
	VariablesValuesRow  PathTemplate `json:"variablesValuesRow" validate:"required"`
}

type PathTemplate string

func (p PathTemplate) MatchPath(path string) (bool, map[string]string) {
	r := p.regexp()
	result := r.FindStringSubmatch(path)
	if result == nil {
		return false, nil
	}

	// Convert result to map
	matches := make(map[string]string)
	for i, name := range r.SubexpNames() {
		if i != 0 && name != "" {
			matches[name] = result[i]
		}
	}
	return true, matches
}

func (p PathTemplate) regexp() *regexp.Regexp {
	// Replace placeholders with regexp groups
	str := regexp.QuoteMeta(string(p))
	str = regexpcache.
		MustCompile(`\\\{[^{}]+\\\}`).
		ReplaceAllStringFunc(str, p.placeholderToRegexp)

	// Config and row ID can be missing
	optional := []string{"config_id", "config_row_id"}
	for _, name := range optional {
		str = regexpcache.
			MustCompile(fmt.Sprintf(`[^/()]*%s[^/()]*`, regexp.QuoteMeta(p.placeholderToRegexp(name)))).
			ReplaceAllString(str, `(?:$0)?`)
	}

	// Compile regexp
	return regexpcache.MustCompile(`^` + str + `$`)
}

func (p PathTemplate) placeholderToRegexp(placeholder string) string {
	return `(?P<` + strings.Trim(placeholder, `\{}`) + `>[^/]+)`
}

func TemplateWithoutIds() Template {
	return Template{
		Branch:              "{branch_name}",
		Config:              "{component_type}/{component_id}/{config_name}",
		ConfigRow:           "rows/{config_row_name}",
		SchedulerConfig:     "schedules/{config_name}",
		SharedCodeConfig:    "_shared/{target_component_id}",
		SharedCodeConfigRow: "codes/{config_row_name}",
		VariablesConfig:     "variables",
		VariablesValuesRow:  "values/{config_row_name}",
	}
}

func TemplateWithIds() Template {
	return Template{
		Branch:              "{branch_id}-{branch_name}",
		Config:              "{component_type}/{component_id}/{config_id}-{config_name}",
		ConfigRow:           "rows/{config_row_id}-{config_row_name}",
		SchedulerConfig:     "schedules/{config_id}-{config_name}",
		SharedCodeConfig:    "_shared/{target_component_id}",
		SharedCodeConfigRow: "codes/{config_row_id}-{config_row_name}",
		VariablesConfig:     "variables",
		VariablesValuesRow:  "values/{config_row_id}-{config_row_name}",
	}
}
