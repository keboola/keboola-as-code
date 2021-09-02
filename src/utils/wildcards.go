package utils

import (
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/pmezard/go-difflib/difflib"
	"github.com/stretchr/testify/assert"
)

// AssertWildcards compares two texts, in expected value can be used wildcards, see WildcardToRegexp function.
func AssertWildcards(t assert.TestingT, expected string, actual string, msg string) {
	expected = strings.TrimSpace(expected)
	actual = strings.TrimSpace(actual)

	// Replace NBSP with space
	actual = strings.ReplaceAll(actual, " ", " ")

	// Assert
	if len(expected) == 0 {
		assert.Equal(t, expected, actual, msg)
	} else {
		expectedRegexp := WildcardToRegexp(strings.TrimSpace(expected))
		diff := difflib.UnifiedDiff{
			A: difflib.SplitLines(EscapeWhitespaces(expected)),
			B: difflib.SplitLines(EscapeWhitespaces(actual)),
		}
		diffStr, _ := difflib.GetUnifiedDiffString(diff)
		if len(diffStr) > 0 {
			assert.Regexp(t, "^"+expectedRegexp+"$", actual, msg+" Diff:\n"+diffStr)
		}
	}
}

// WildcardToRegexp converts string with wildcards to regexp, so it can be used in assert.Regexp.
func WildcardToRegexp(pattern string) string {
	pattern = regexp.QuoteMeta(pattern)
	re := regexp.MustCompile(`%.`)
	return re.ReplaceAllStringFunc(pattern, func(s string) string {
		// Inspired by PhpUnit method "assertStringMatchesFormat"
		// https://phpunit.readthedocs.io/en/9.5/assertions.html#assertstringmatchesformat
		switch s {
		// %e: Represents a directory separator, for example / on Linux.
		case `%e`:
			return string(os.PathSeparator)
		// %s: One or more of anything (character or white space) except the end of line character.
		case `%s`:
			return `.+`
		// %S: Zero or more of anything (character or white space) except the end of line character.
		case `%S`:
			return `.*`
		// %a: One or more of anything (character or white space) including the end of line character.
		case `%a`:
			return `(.|\n)+`
		// %A: Zero or more of anything (character or white space) including the end of line character.
		case `%A`:
			return `(.|\n)*`
		// %w: Zero or more white space characters.
		case `%w`:
			return `\s*`
		// %i: A signed integer value, for example +3142, -3142.
		case `%i`:
			return `(\+|\-)\d+`
		// %d: An unsigned integer value, for example 123456.
		case `%d`:
			return `\d+`
		// %x: One or more hexadecimal character. That is, characters in the range 0-9, a-f, A-F.
		case `%x`:
			return `[0-9a-zA-Z]+`
		// %f: A floating point number, for example: 3.142, -3.142, 3.142E-10, 3.142e+10.
		case `%f`:
			return `[-+]?[0-9]*\.?[0-9]+([eE][-+]?[0-9]+)?`
		// %c: A single character of any sort.
		case `%c`:
			return `.`
		// %%: A literal percent character: %.
		case `%%`:
			return `%`
		}

		return s
	})
}

// EscapeWhitespaces escapes all whitespaces except new line -> for better difference in diff output.
func EscapeWhitespaces(input string) string {
	re := regexp.MustCompile(`\s`)
	return re.ReplaceAllStringFunc(input, func(s string) string {
		if s == "\n" {
			return s
		} else if s == "\t" {
			return `→→→→`
		} else if s == " " {
			return `␣`
		}
		return strings.Trim(strconv.Quote(s), `"`)
	})
}
