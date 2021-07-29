package utils

import (
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/umisama/go-regexpcache"
)

type PathTemplate string

func (p *PathTemplate) MatchPath(path string) (bool, map[string]string) {
	path = strings.ReplaceAll(path, string(os.PathSeparator), "/")
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

func (p *PathTemplate) regexp() *regexp.Regexp {
	// Replace placeholders with regexp groups
	str := regexp.QuoteMeta(string(*p))
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

func (p *PathTemplate) placeholderToRegexp(placeholder string) string {
	return `(?P<` + strings.Trim(placeholder, `\{}`) + `>[^/]+)`
}

type KeyPath []Step

type Step interface {
	String() string
}

type MapStep string

type SliceStep int

func (v KeyPath) String() string {
	parts := make([]string, 0)
	for _, step := range v {
		parts = append(parts, step.String())
	}
	return strings.Join(parts, ".")
}

func (v MapStep) String() string {
	return string(v)
}

func (v SliceStep) String() string {
	return fmt.Sprintf("[%v]", int(v))
}
