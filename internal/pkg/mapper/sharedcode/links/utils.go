package links

import (
	"fmt"
	"regexp"
	"strings"
)

func (m *mapper) matchId(script string) string {
	script = strings.TrimSpace(script)
	match := m.idRegexp.FindStringSubmatch(script)
	if len(match) > 0 {
		return match[1]
	}
	return ""
}

func (m *mapper) matchPath(script, componentId string) string {
	comment := m.Naming.CodeFileComment(m.Naming.CodeFileExt(componentId))
	script = strings.TrimSpace(script)
	script = strings.TrimPrefix(script, comment)
	script = strings.TrimSpace(script)
	match := m.pathRegexp.FindStringSubmatch(script)
	if len(match) > 0 {
		return match[1]
	}
	return ""
}

func (m *mapper) formatId(id string) string {
	placeholder := strings.ReplaceAll(IdFormat, `<ID>`, id)
	if ok := m.idRegexp.MatchString(placeholder); !ok {
		panic(fmt.Errorf(`shared code id "%s" is invalid`, id))
	}
	return placeholder
}

func (m *mapper) formatPath(path, componentId string) string {
	placeholder := strings.ReplaceAll(PathFormat, `<PATH>`, path)
	if ok := m.pathRegexp.MatchString(placeholder); !ok {
		panic(fmt.Errorf(`shared code path "%s" is invalid`, path))
	}
	comment := m.Naming.CodeFileComment(m.Naming.CodeFileExt(componentId))
	return comment + ` ` + placeholder
}

func idRegexp() *regexp.Regexp {
	return regexp.MustCompile(
		strings.ReplaceAll(
			`^`+regexp.QuoteMeta(IdFormat)+`$`,
			`<ID>`,
			`(`+IdRegexp+`)`,
		),
	)
}

func pathRegexp() *regexp.Regexp {
	return regexp.MustCompile(
		strings.ReplaceAll(
			`^`+regexp.QuoteMeta(PathFormat)+`$`,
			`<PATH>`,
			`(`+PathRegexp+`)`,
		),
	)
}
