package log

import (
	"strings"
)

func Sanitize(in string) string {
	out := strings.ReplaceAll(in, "\n", `\n`)
	return strings.ReplaceAll(out, "\r", `\n`)
}
