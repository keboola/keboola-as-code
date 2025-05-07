package links

import (
	"regexp"
	"strings"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	IDFormat = `{{<ID>}}` // link to shared code used in API
	IDRegexp = `[0-9a-zA-Z_\-]+`
)

type idUtils struct {
	re *regexp.Regexp
}

func newIDUtils() *idUtils {
	re := regexp.MustCompile(
		strings.ReplaceAll(
			`^`+regexp.QuoteMeta(IDFormat)+`$`,
			`<ID>`,
			`(`+IDRegexp+`)`,
		),
	)
	return &idUtils{re: re}
}

func (v *idUtils) match(script string) keboola.RowID {
	script = strings.TrimSpace(script)
	match := v.re.FindStringSubmatch(script)
	if len(match) > 0 {
		return keboola.RowID(match[1])
	}
	return ""
}

func (v *idUtils) format(id keboola.RowID) string {
	placeholder := strings.ReplaceAll(IDFormat, `<ID>`, id.String())
	if ok := v.re.MatchString(placeholder); !ok {
		panic(errors.Errorf(`shared code id "%s" is invalid`, id))
	}
	return placeholder
}
