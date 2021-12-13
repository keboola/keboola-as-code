package links

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func (m *mapper) sharedCodeRowByScriptId(code *model.Code, script string, sharedCode model.ConfigKey) (*model.ConfigRowState, error) {
	id := m.matchId(script)
	if id == "" {
		// Not found
		return nil, nil
	}

	// Get shared code config row
	rowKey := model.ConfigRowKey{
		BranchId:    sharedCode.BranchId,
		ComponentId: sharedCode.ComponentId,
		ConfigId:    sharedCode.Id,
		Id:          id,
	}
	row, found := m.State.Get(rowKey)
	if !found {
		return nil, utils.PrefixError(
			fmt.Sprintf(`missing shared code %s`, rowKey.Desc()),
			fmt.Errorf(`referenced from %s`, code.Path()),
		)
	}

	return row.(*model.ConfigRowState), nil
}

func (m *mapper) matchId(script string) model.RowId {
	script = strings.TrimSpace(script)
	match := m.idRegexp.FindStringSubmatch(script)
	if len(match) > 0 {
		return model.RowId(match[1])
	}
	return ""
}

func (m *mapper) matchPath(script string, componentId model.ComponentId) string {
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
