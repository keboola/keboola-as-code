package terminal

import (
	"bytes"
	"testing"

	"github.com/fatih/color"
	"github.com/stretchr/testify/assert"
)

func TestStringWithoutANSI(t *testing.T) {
	t.Parallel()

	m := stringWithoutANSIMatcher{str: "foo"}

	assert.True(t, m.Match(bytes.NewBufferString("foo")))
	assert.True(t, m.Match(bytes.NewBufferString(">>>foo<<")))
	assert.True(t, m.Match(bytes.NewBufferString(
		color.RedString("f")+color.GreenString("o")+color.BlueString("o"),
	)))

	assert.False(t, m.Match(bytes.NewBufferString(">>>bar<<")))
	assert.False(t, m.Match(bytes.NewBufferString(">>>f\no\no\n<<")))
	assert.False(t, m.Match(bytes.NewBufferString(
		color.RedString("b")+color.GreenString("a")+color.BlueString("r"),
	)))
}
