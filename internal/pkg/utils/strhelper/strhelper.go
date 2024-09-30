package strhelper

import (
	"bufio"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"unicode"

	"github.com/jpillora/longestcommon"
	"github.com/spf13/cast"
	"github.com/umisama/go-regexpcache"
	stripmd "github.com/writeas/go-strip-markdown"
	"golang.org/x/text/runes"
	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"
)

// FormatPathChange - example result "branch/config/{row -> row1}".
func FormatPathChange(src string, dst string, quote bool) string {
	q := ``
	if quote {
		q = `"`
	}

	prefix, src, dst := ExtractCommonPrefix(src, dst)
	if prefix != "" && len(prefix) > 2 {
		return fmt.Sprintf(`%s%s{%s -> %s}%s`, q, prefix, src, dst, q)
	} else {
		return fmt.Sprintf(`%s%s%s -> %s%s%s`, q, src, q, q, dst, q)
	}
}

// ExtractCommonPrefix from two strings.
// Returns prefix, first string without prefix and second string without prefix.
func ExtractCommonPrefix(a string, b string) (prefix string, ax string, bx string) {
	prefix = longestcommon.Prefix([]string{a, b})

	// Remove from the prefix everything after the last separator
	seps := regexp.QuoteMeta(`\/`)
	prefix = regexpcache.
		MustCompile(fmt.Sprintf(`(^|[%s])[^%s]*$`, seps, seps)).
		ReplaceAllString(prefix, "$1")

	// Prefix length > 2 AND must leave some different non-empty leftovers
	ax = strings.TrimPrefix(a, prefix)
	bx = strings.TrimPrefix(b, prefix)
	if len(prefix) > 2 && len(ax) > 0 && len(bx) > 0 && ax != bx {
		return prefix, ax, bx
	}

	return "", a, b
}

func MatchWords(value string, wordsStr string) bool {
	wordsStr = strings.TrimSpace(strings.ToLower(wordsStr))
	words := strings.Split(wordsStr, " ")
	for _, word := range words {
		word = strings.TrimSpace(word)
		if len(word) == 0 {
			continue
		}

		if !strings.Contains(value, word) {
			return false
		}
	}

	return true
}

func FirstLower(str string) string {
	return strings.ToLower(string(str[0])) + str[1:]
}

func FirstUpper(str string) string {
	if len(str) == 0 {
		return str
	}
	return strings.ToUpper(string(str[0])) + str[1:]
}

// StripHTMLComments replaces HTML comments with empty lines.
func StripHTMLComments(str string) string {
	return regexpcache.
		MustCompile("(?s)<!--(.*?)-->").
		ReplaceAllStringFunc(str, func(s string) string {
			// Replace comment with empty lines
			return strings.Repeat("\n", strings.Count(s, "\n"))
		})
}

func Truncate(str string, maximum int, suffix string) string {
	if len(str) <= maximum {
		return str
	}
	return str[0:maximum] + suffix
}

// NormalizeName converts any string into kebab-case.
func NormalizeName(str string) string {
	// Prepend all uppercase letters with separator
	// "--CamelCase" -> "---Camel-Case"
	str = regexpcache.
		MustCompile(`([A-Z]+)`).
		ReplaceAllString(str, "-$1")
	// Replace special characters with one separator
	// "---Camel-Case" -> "-Camel-Case"
	str = regexpcache.
		MustCompile(`[^a-zA-Zá-žÁ-Ž0-9]+`).
		ReplaceAllString(str, "-")
	// Normalize accented letters
	t := transform.Chain(norm.NFD, runes.Remove(runes.In(unicode.Mn)), norm.NFC)
	str, _, _ = transform.String(t, str)
	// Trim separators
	// "-Camel-Case" -> "Camel-Case"
	str = strings.Trim(str, "-")
	// Convert to lower
	// "Camel-Case" -> "camel-case"
	str = strings.ToLower(str)
	return str
}

func NormalizeHost(host string) string {
	host = strings.TrimRight(host, "/")
	host = strings.TrimPrefix(host, "https://")
	host = strings.TrimPrefix(host, "http://")
	return host
}

func FirstN(str string, n int) string {
	if n > len(str) {
		n = len(str)
	}
	return str[0:n]
}

func StripMarkdown(str string) string {
	return stripmd.Strip(str)
}

func MustURLPathUnescape(in string) string {
	out, err := url.PathUnescape(in)
	if err != nil {
		return in
	}
	return out
}

func ReplacePlaceholders(path string, placeholders map[string]any) string {
	for key, value := range placeholders {
		path = strings.ReplaceAll(path, "{"+key+"}", cast.ToString(value))
	}
	return path
}

func AsSentence(msg string) string {
	out := strings.TrimRight(msg, " ")
	if len(out) == 0 {
		return msg
	}

	// First letter is uppercase.
	out = FirstUpper(out)

	// Dot is added to the end, if message doesn't end with a special character.
	if regexpcache.MustCompile(`[a-zA-Z0-9'"]$`).MatchString(out) {
		out += "."
	}
	return out
}

func FilterLines(keep, lines string) string {
	var out strings.Builder
	exp := regexpcache.MustCompile(keep)
	s := bufio.NewScanner(strings.NewReader(lines))
	for s.Scan() {
		line := s.Text()
		if exp.MatchString(line) {
			out.WriteString(line)
			out.WriteString("\n")
		}
	}
	if err := s.Err(); err != nil {
		panic(err)
	}
	return strings.TrimRight(out.String(), "\n")
}
