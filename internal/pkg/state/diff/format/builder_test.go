package format_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	. "github.com/keboola/keboola-as-code/internal/pkg/state/diff/format"
)

func TestBuilder(t *testing.T) {
	b := NewBuilder()

	b.WriteString("foo\n")
	b.WriteString("bar\n")
	b.WritePlaceholder("<default value>", func(f PathFormatter) string {
		return "<dynamic value>"
	})
	b.WriteString("\n")
	b.WriteString("baz\n")

	// Value for diff generation
	assert.Equal(t, strings.TrimLeft(`
foo
bar
<default value>
baz
`, "\n"), b.Transform())

	// Value for diff formatting
	assert.Equal(t, strings.TrimLeft(`
foo
bar
<dynamic value>
baz
`, "\n"), b.Format(nil))

	// Finalize fn
	b.FinalizeFn(func(str string) string {
		return str + "<finalize>\n"
	})

	// Value for diff generation
	assert.Equal(t, strings.TrimLeft(`
foo
bar
<default value>
baz
<finalize>
`, "\n"), b.Transform())

	// Value for diff formatting
	assert.Equal(t, strings.TrimLeft(`
foo
bar
<dynamic value>
baz
<finalize>
`, "\n"), b.Format(nil))
}
