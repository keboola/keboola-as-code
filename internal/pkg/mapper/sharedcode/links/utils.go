package links

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
)

func (m *mapper) matchId(script string) model.RowId {
	script = strings.TrimSpace(script)
	match := m.idRegexp.FindStringSubmatch(script)
	if len(match) > 0 {
		return model.RowId(match[1])
	}
	return ""
}

func (m *mapper) matchPath(script string, componentId model.ComponentId) string {
	comment := naming.CodeFileComment(naming.CodeFileExt(componentId))
	script = strings.TrimSpace(script)
	script = strings.TrimPrefix(script, comment)
	script = strings.TrimSpace(script)
	match := m.pathRegexp.FindStringSubmatch(script)
	if len(match) > 0 {
		return match[1]
	}
	return ""
}

func (m *mapper) formatId(id model.RowId) string {
	placeholder := strings.ReplaceAll(IdFormat, `<ID>`, id.String())
	if ok := m.idRegexp.MatchString(placeholder); !ok {
		panic(fmt.Errorf(`shared code id "%s" is invalid`, id))
	}
	return placeholder
}

func (m *mapper) formatPath(path string, componentId model.ComponentId) string {
	placeholder := strings.ReplaceAll(PathFormat, `<PATH>`, path)
	if ok := m.pathRegexp.MatchString(placeholder); !ok {
		panic(fmt.Errorf(`shared code path "%s" is invalid`, path))
	}
	comment := naming.CodeFileComment(naming.CodeFileExt(componentId))
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
