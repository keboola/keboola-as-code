package strhelper

import (
	"testing"

	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"
)

func TestFormatPathChange(t *testing.T) {
	cases := []struct {
		src      string
		dst      string
		expected string
	}{
		{`abc`, `abc`, `"abc" -> "abc"`},
		{`abc`, `123`, `"abc" -> "123"`},
		{`abc`, `abC`, `"abc" -> "abC"`},
		{`/a/b/c`, `/a/b/c`, `"/a/b/c" -> "/a/b/c"`},
		{`/a/b/c`, `/a/b/D`, `"/a/b/{c -> D}"`},
		{`/a/b/c`, `/a/D/c`, `"/a/{b/c -> D/c}"`},
		{`branch/config/foo`, `branch/config/bar`, `"branch/config/{foo -> bar}"`},
		{`branch/config/row`, `branch/config/row1`, `"branch/config/{row -> row1}"`},
		{`branch\config\row`, `branch\config\row1`, `"branch\config\{row -> row1}"`},
		{`branch/config1/row`, `branch/config2/row1`, `"branch/{config1/row -> config2/row1}"`},
		{`branch1/config/row`, `branch2/config/row`, `"branch1/config/row" -> "branch2/config/row"`},
	}

	// With quotes
	for cIndex, c := range cases {
		out := FormatPathChange(c.src, c.dst, true)
		assert.Equalf(t, c.expected, out, `case `+cast.ToString(cIndex+1))
	}

	// Without quotes
	assert.Equal(t, "abc -> abc", FormatPathChange("abc", "abc", false))
	assert.Equal(t, "/a/{b/c -> D/c}", FormatPathChange("/a/b/c", "/a/D/c", false))
}

func TestExtractCommonPrefix(t *testing.T) {
	cases := []struct {
		src      string
		dst      string
		expected string
	}{
		{`abc`, `abc`, ``},
		{`abc`, `123`, ``},
		{`abc`, `abC`, ``},
		{`/a/b/c`, `/a/b/c`, ``},
		{`/a/b/c`, `/a/b/D`, `/a/b/`},
		{`/a/b/c`, `/a/D/c`, `/a/`},
		{`branch/config/foo`, `branch/config/bar`, `branch/config/`},
		{`branch/config/row`, `branch/config/row1`, `branch/config/`},
		{`branch/config1/row`, `branch/config2/row1`, `branch/`},
		{`branch1/config/row`, `branch2/config/row`, ``},
	}

	for cIndex, c := range cases {
		out, _, _ := ExtractCommonPrefix(c.src, c.dst)
		assert.Equalf(t, c.expected, out, `case `+cast.ToString(cIndex+1))
	}
}
