package strhelper

import (
	"testing"

	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"
)

func TestFormatPathChange(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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

func TestFilterByWords(t *testing.T) {
	t.Parallel()
	// Match all words from filter, order does not matter
	assert.True(t, MatchWords(``, ``))
	assert.True(t, MatchWords(`foo`, ``))
	assert.True(t, MatchWords(`foo`, `foo`))
	assert.True(t, MatchWords(`foo`, `  foo  `))
	assert.True(t, MatchWords(`   foo   `, `  foo  `))
	assert.True(t, MatchWords(`   foo  bar   `, `  foo  bar `))
	assert.True(t, MatchWords(`foo bar`, `  foo  bar `))
	assert.True(t, MatchWords(`   bar  foo   `, `  foo  bar `))
	assert.True(t, MatchWords(`bar foo`, `  foo  bar `))
	assert.True(t, MatchWords(`bar foo`, `  fo  bar `))

	// All words must be matched
	assert.False(t, MatchWords(`bar`, `foo`))
	assert.False(t, MatchWords(`bar`, `foo bar`))
	assert.False(t, MatchWords(`bar foo`, `  foo  bar baz`))
}

func TestFirstLower(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "foo", FirstLower("foo"))
	assert.Equal(t, "fOO", FirstLower("FOO"))
	assert.Equal(t, "foo", FirstLower("Foo"))
}

func TestFirstUpper(t *testing.T) {
	t.Parallel()
	assert.Empty(t, FirstUpper(""))
	assert.Equal(t, " ", FirstUpper(" "))
	assert.Equal(t, "Foo", FirstUpper("foo"))
	assert.Equal(t, "FOO", FirstUpper("FOO"))
	assert.Equal(t, "Foo", FirstUpper("Foo"))
}

func TestStripHtmlComments(t *testing.T) {
	t.Parallel()

	cases := []struct{ in, expected string }{
		{"", ""},
		{"abc", "abc"},
		{"<!---->", ""},
		{"<!-- -->", ""},
		{"<!-- abc -->", ""},
		{"foo<!-- abc -->", "foo"},
		{"<!-- abc -->bar", "bar"},
		{"foo<!-- abc -->bar", "foobar"},
		{"foo\n<!-- abc -->\nbar", "foo\n\nbar"},
		{"foo\n<!-- abc\ndef -->\nbar", "foo\n\n\nbar"},
		{"foo\n<!-- abc\ndef -->\nbar<!-- abc\ndef -->", "foo\n\n\nbar\n"},
	}

	for i, c := range cases {
		assert.Equal(t, c.expected, StripHTMLComments(c.in), "case "+cast.ToString(i))
	}
}

func TestTruncate(t *testing.T) {
	t.Parallel()

	cases := []struct {
		in       string
		max      int
		suffix   string
		expected string
	}{
		{"", 5, "", ""},
		{"abc", 5, "", "abc"},
		{"abcde", 5, "", "abcde"},
		{"abcdef", 5, "", "abcde"},
		{"abc", 5, "…", "abc"},
		{"abcde", 5, "…", "abcde"},
		{"abcdef", 5, "…", "abcde…"},
	}

	for i, c := range cases {
		assert.Equal(t, c.expected, Truncate(c.in, c.max, c.suffix), "case "+cast.ToString(i))
	}
}

func TestNamingNormalizeName(t *testing.T) {
	t.Parallel()
	assert.Empty(t, NormalizeName(""))
	assert.Equal(t, "abc", NormalizeName("abc"))
	assert.Equal(t, "camel-case", NormalizeName("CamelCase"))
	assert.Equal(t, "space-separated", NormalizeName("   space   separated  "))
	assert.Equal(t, "abc-def-xyz", NormalizeName("__abc_def_xyz___"))
	assert.Equal(t, "abc-dev-xyz", NormalizeName("--abc-dev-xyz---"))
	assert.Equal(t, "a-b-cd-e-f-x-y-z", NormalizeName("a B cd-eF   x_y___z__"))
	assert.Equal(t, "ex-generic-v2", NormalizeName("ex-generic-v2"))
}

func TestFirstN(t *testing.T) {
	t.Parallel()
	assert.Empty(t, FirstN("", 0))
	assert.Empty(t, FirstN("", 100))
	assert.Empty(t, FirstN("a", 0))
	assert.Equal(t, "a", FirstN("a", 100))
	assert.Empty(t, FirstN("abcde", 0))
	assert.Equal(t, "a", FirstN("abcde", 1))
	assert.Equal(t, "ab", FirstN("abcde", 2))
	assert.Equal(t, "abc", FirstN("abcde", 3))
	assert.Equal(t, "abcd", FirstN("abcde", 4))
	assert.Equal(t, "abcde", FirstN("abcde", 5))
	assert.Equal(t, "abcde", FirstN("abcde", 6))
}

func TestStripMarkdown(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "unchanged\ntest", StripMarkdown("unchanged\ntest"))
	assert.Equal(t, "heading", StripMarkdown("### heading"))
	assert.Equal(t, "link", StripMarkdown("[link](https://google.com/?a=b&c=d#anchor)"))
}

func TestReplacePlaceholders(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "foo", ReplacePlaceholders("foo", map[string]any{}))
	assert.Equal(t, "foo", ReplacePlaceholders("foo", map[string]any{"foo": "bar"}))
	assert.Equal(t, "bar", ReplacePlaceholders("{foo}", map[string]any{"foo": "bar"}))
	assert.Equal(t, "AbarB", ReplacePlaceholders("A{foo}B", map[string]any{"foo": "bar"}))
}

func TestAsSentence(t *testing.T) {
	t.Parallel()

	cases := []struct {
		in       string
		expected string
	}{
		{"", ""},
		{" ", " "},
		{"1", "1."},
		{"a", "A."},
		{"A", "A."},
		{"foo bar", "Foo bar."},
		{"foo bar ", "Foo bar."},
		{"foo bar.", "Foo bar."},
		{"Foo bar", "Foo bar."},
		{"Foo bar.", "Foo bar."},
		{"foo bar:", "Foo bar:"},
		{"Foo bar:", "Foo bar:"},
		{"foo bar>", "Foo bar>"},
		{"Foo bar>", "Foo bar>"},
		{`foo bar "xyz"`, `Foo bar "xyz".`},
	}

	for i, c := range cases {
		assert.Equal(t, c.expected, AsSentence(c.in), "case "+cast.ToString(i))
	}
}

func TestFilterLines(t *testing.T) {
	t.Parallel()

	cases := []struct {
		in       string
		expected string
	}{
		{"", ""},
		{" ", ""},
		{"<KEEP>", "<KEEP>"},
		{"<KEEP> foo", "<KEEP> foo"},
		{"<KEEP> foo\n", "<KEEP> foo"},
		{"\n\n\n<KEEP> foo\n\n\n", "<KEEP> foo"},
		{"foo\n<KEEP> a\nfoo\n<KEEP> b\nfoo\n<KEEP> c\n", "<KEEP> a\n<KEEP> b\n<KEEP> c"},
	}

	for i, c := range cases {
		assert.Equal(t, c.expected, FilterLines("^<KEEP>", c.in), "case "+cast.ToString(i))
	}
}
