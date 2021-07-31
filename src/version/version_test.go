package version

import (
	"regexp"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestVersion(t *testing.T) {
	assert.Regexp(
		t,
		`^`+
			`Version:.*\n`+
			`Git commit:.*\n`+
			`Build date:.*\n`+
			`Go version:\s+`+regexp.QuoteMeta(runtime.Version())+`\n`+
			`Os/Arch:\s+`+regexp.QuoteMeta(runtime.GOOS)+`/`+regexp.QuoteMeta(runtime.GOARCH)+`\n`+
			`$`,
		Version(),
	)
}
