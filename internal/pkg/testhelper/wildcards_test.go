package testhelper

import (
	"bytes"
	"fmt"
	"os"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAssertWildcardsSame1(t *testing.T) {
	t.Parallel()
	test := &mockedT{buf: bytes.NewBuffer(nil)}
	AssertWildcards(test, "foo", "foo", "Fail msg.")
	assert.Equal(t, "", test.buf.String())
}

func TestAssertWildcardsSame2(t *testing.T) {
	t.Parallel()
	test := &mockedT{buf: bytes.NewBuffer(nil)}
	AssertWildcards(test, "%c%c%c", "foo", "Fail msg.")
	assert.Equal(t, "", test.buf.String())
}

func TestAssertWildcardsDifferent1(t *testing.T) {
	t.Parallel()
	test := &mockedT{buf: bytes.NewBuffer(nil)}
	AssertWildcards(test, "foo", "bar", "Fail msg.")
	assert.Contains(t, test.buf.String(), "Expect \"bar\" to match \"^foo$\"")
}

func TestAssertWildcardsDifferent2(t *testing.T) {
	t.Parallel()
	test := &mockedT{buf: bytes.NewBuffer(nil)}
	AssertWildcards(test, "%c%c%c%c", "bar", "Fail msg.")
	assert.Contains(t, test.buf.String(), "Expect \"bar\" to match \"^....$\"")
}

func TestWildcardToRegexp(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in  string
		out string
	}{
		{in: ``, out: ``},
		{in: `%e`, out: regexp.QuoteMeta(string(os.PathSeparator))}, // nolint forbidigo
		{in: `%s`, out: `.+`},
		{in: `%S`, out: `.*`},
		{in: `%a`, out: `(.|\n)+`},
		{in: `%A`, out: `(.|\n)*`},
		{in: `%w`, out: `\s*`},
		{in: `%i`, out: `(\+|\-)\d+`},
		{in: `%d`, out: `\d+`},
		{in: `%x`, out: `[0-9a-zA-Z]+`},
		{in: `%f`, out: `[-+]?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?`},
		{in: `%c`, out: `.`},
		{in: `%%`, out: `%`},
	}

	for _, data := range cases {
		assert.Equal(t, data.out, WildcardToRegexp(data.in), data.in)
	}
}

func TestWildcardToRegexpMatch(t *testing.T) {
	t.Parallel()
	cases := []struct {
		pattern string
		input   string
		match   bool
	}{
		{pattern: ``, input: `foo`, match: false},
		{pattern: ``, input: ``, match: true},
		{pattern: `%e`, input: `foo`, match: false},
		{pattern: `%e`, input: string(os.PathSeparator), match: true}, // nolint forbidigo
		{pattern: `%s`, input: ``, match: false},
		{pattern: `%s`, input: "\n", match: false},
		{pattern: `%s`, input: ` `, match: true},
		{pattern: `%s`, input: `x`, match: true},
		{pattern: `%s`, input: `foo`, match: true},
		{pattern: `%S`, input: "\n", match: false},
		{pattern: `%S`, input: ``, match: true},
		{pattern: `%S`, input: ` `, match: true},
		{pattern: `%S`, input: `x`, match: true},
		{pattern: `%S`, input: `foo`, match: true},
		{pattern: `%a`, input: ``, match: false},
		{pattern: `%a`, input: "\n", match: true},
		{pattern: `%a`, input: ` `, match: true},
		{pattern: `%a`, input: `x`, match: true},
		{pattern: `%a`, input: `foo`, match: true},
		{pattern: `%A`, input: "\n", match: true},
		{pattern: `%A`, input: ``, match: true},
		{pattern: `%A`, input: ` `, match: true},
		{pattern: `%A`, input: `x`, match: true},
		{pattern: `%A`, input: `foo`, match: true},
		{pattern: `%w`, input: ``, match: true},
		{pattern: `%w`, input: ` `, match: true},
		{pattern: `%w`, input: " \t\n", match: true},
		{pattern: `%i`, input: ``, match: false},
		{pattern: `%i`, input: `123`, match: false},
		{pattern: `%i`, input: `+123`, match: true},
		{pattern: `%i`, input: `-123`, match: true},
		{pattern: `%d`, input: ``, match: false},
		{pattern: `%d`, input: `123`, match: true},
		{pattern: `%d`, input: `+123`, match: false},
		{pattern: `%d`, input: `-123`, match: false},
		{pattern: `%x`, input: ``, match: false},
		{pattern: `%x`, input: `0af`, match: true},
		{pattern: `%f`, input: ``, match: false},
		{pattern: `%f`, input: `12`, match: true},
		{pattern: `%f`, input: `12.34`, match: true},
		{pattern: `%f`, input: `+12.34`, match: true},
		{pattern: `%f`, input: `-12.34`, match: true},
		{pattern: `%c`, input: ``, match: false},
		{pattern: `%c`, input: `aa`, match: false},
		{pattern: `%c`, input: `a`, match: true},
		{pattern: `%%`, input: ``, match: false},
		{pattern: `%%`, input: `x`, match: false},
		{pattern: `%%`, input: `%`, match: true},
	}

	for _, data := range cases {
		match := regexp.MustCompile(`^` + WildcardToRegexp(data.pattern) + `$`).MatchString(data.input)
		assert.Equal(t, data.match, match, fmt.Sprintf(`pattern: "%s", input: "%s"`, data.pattern, data.input))
	}
}
