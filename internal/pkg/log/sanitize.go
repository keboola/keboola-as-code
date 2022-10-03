package log

import (
	"strings"
)

func Sanitize(in string) string {
	out := strings.Replace(in, "\n", `\n`, -1)
	return strings.Replace(out, "\r", `\n`, -1)
}
