package links

import (
	"fmt"
	"regexp"
	"strings"
)

const (
	IdFormat = `{{<ID>}}` // link to shared code used in API
	IdRegexp = `[0-9a-zA-Z_\-]+`
)

type idUtils struct {
	re *regexp.Regexp
}

func newIdUtils() *idUtils {
	re := regexp.MustCompile(
		strings.ReplaceAll(
			`^`+regexp.QuoteMeta(IdFormat)+`$`,
			`<ID>`,
			`(`+IdRegexp+`)`,
		),
	)
	return &idUtils{re: re}
}

func (v *idUtils) match(script string) storageapi.RowID {
	script = strings.TrimSpace(script)
	match := v.re.FindStringSubmatch(script)
	if len(match) > 0 {
		return storageapi.RowID(match[1])
	}
	return ""
}

func (v *idUtils) format(id storageapi.RowID) string {
	placeholder := strings.ReplaceAll(IdFormat, `<ID>`, id.String())
	if ok := v.re.MatchString(placeholder); !ok {
		panic(fmt.Errorf(`shared code id "%s" is invalid`, id))
	}
	return placeholder
}
