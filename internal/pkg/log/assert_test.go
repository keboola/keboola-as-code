package log

import (
	"bytes"
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type mockedT struct {
	buf *bytes.Buffer
}

// Implements TestingT for mockedT.
func (t *mockedT) Errorf(format string, args ...any) {
	s := fmt.Sprintf(format, args...)
	t.buf.WriteString(s)
}

func TestCompareJsonMessages(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		expected string
		actual   string
		err      error
	}{
		{
			name:     "empty",
			expected: "{}",
			actual:   "{}",
		},
		{
			name:     "invalid expected",
			expected: "invalid",
			actual:   "{}",
			err:      errors.New("expected string contains invalid json:\ninvalid"),
		},
		{
			name:     "invalid actual",
			expected: "{}",
			actual:   "invalid",
			err:      errors.New("actual string contains invalid json:\ninvalid"),
		},
		{
			name:     "ignore extra fields",
			expected: `{"level":"info","message":"Info msg"}`,
			actual:   `{"level":"info","message":"Info msg","extra":"value"}`,
		},
		{
			name:     "ignore extra lines",
			expected: `{"level":"info","message":"Info msg"}`,
			actual: `
{"level":"debug","message":"Debug msg"}
{"level":"info","message":"Info msg"}
`,
		},
		{
			name:     "warning cannot be ignored - before expected message",
			expected: `{"level":"info","message":"Info msg"}`,
			actual: `
{"level":"debug","message":"Debug msg"}
{"level":"warn","message":"Warn msg"}
{"level":"info","message":"Info msg"}
`,
			err: errors.Errorf(
				"Expected:\n-----\n%s\n-----\nFound unexpected message:\n-----\n%s",
				`{"level":"info","message":"Info msg"}`,
				`{"level":"warn","message":"Warn msg"}`,
			),
		},
		{
			name:     "error cannot be ignored - before expected message",
			expected: `{"level":"info","message":"Info msg"}`,
			actual: `
{"level":"debug","message":"Debug msg"}
{"level":"error","message":"Error msg"}
{"level":"info","message":"Info msg"}
`,
			err: errors.Errorf(
				"Expected:\n-----\n%s\n-----\nFound unexpected message:\n-----\n%s",
				`{"level":"info","message":"Info msg"}`,
				`{"level":"error","message":"Error msg"}`,
			),
		},
		{
			name:     "warning cannot be ignored - after expected message",
			expected: `{"level":"info","message":"Info msg"}`,
			actual: `
{"level":"debug","message":"Debug msg"}
{"level":"info","message":"Info msg"}
{"level":"warn","message":"Warn msg"}
`,
			err: errors.Errorf(
				"Expected:\n-----\n%s\n-----\nFound unexpected message:\n-----\n%s",
				`<nothing>`,
				`{"level":"warn","message":"Warn msg"}`,
			),
		},
		{
			name:     "error cannot be ignored - after expected message",
			expected: `{"level":"info","message":"Info msg"}`,
			actual: `
{"level":"debug","message":"Debug msg"}
{"level":"info","message":"Info msg"}
{"level":"error","message":"Error msg"}
`,
			err: errors.Errorf(
				"Expected:\n-----\n%s\n-----\nFound unexpected message:\n-----\n%s",
				`<nothing>`,
				`{"level":"error","message":"Error msg"}`,
			),
		},
		{
			name:     "wildcard match",
			expected: `{"level":"info","message":"Info %s"}`,
			actual:   `{"level":"info","message":"Info msg"}`,
		},
		{
			name:     "non-string match",
			expected: `{"level":"info","message":"Info %s","count":5}`,
			actual:   `{"level":"info","message":"Info msg","count":5}`,
		},
		{
			name:     "non-string mismatch",
			expected: `{"level":"info","message":"Info %s","count":true}`,
			actual:   `{"level":"info","message":"Info msg","count":false}`,
			err: errors.Errorf(
				"Expected:\n-----\n%s\n-----\nActual:\n-----\n%s",
				`{"level":"info","message":"Info %s","count":true}`,
				`{"level":"info","message":"Info msg","count":false}`,
			),
		},
		{
			name:     "field mismatch",
			expected: `{"level":"info","message":"Info %s"}`,
			actual:   `{"level":"info","message":"msg"}`,
			err: errors.Errorf(
				"Expected:\n-----\n%s\n-----\nActual:\n-----\n%s",
				`{"level":"info","message":"Info %s"}`,
				`{"level":"info","message":"msg"}`,
			),
		},
		{
			name: "match multiple lines",
			expected: `
{"level":"info","message":"Opened file", "file":"file2"}
{"level":"info","message":"Opened file", "file":"file4"}
`,
			actual: `
{"level":"info","message":"Opened file", "file":"file1"}
{"level":"info","message":"Opened file", "file":"file2"}
{"level":"info","message":"Opened file", "file":"file3"}
{"level":"info","message":"Opened file", "file":"file4"}
{"level":"info","message":"Opened file", "file":"file5"}
`,
		},
		{
			name: "match multiple lines - fail",
			expected: `
{"level":"info","message":"Opened file", "file":"file2"}
{"level":"info","message":"Opened file", "file":"file4"}
{"level":"info","message":"Opened file", "file":"file3"}
`,
			actual: `
{"level":"info","message":"Opened file", "file":"file1"}
{"level":"info","message":"Opened file", "file":"file2"}
{"level":"info","message":"Opened file", "file":"file3"}
{"level":"info","message":"Opened file", "file":"file4"}
{"level":"info","message":"Opened file", "file":"file5"}
`,
			err: errors.Errorf(
				"Expected:\n-----\n%s\n-----\nNote: %s\n%s\nActual:\n-----\n%s",
				`{"level":"info","message":"Opened file", "file":"file3"}`,
				"The expected message exists in the log but above the point where it is expected.",
				"If the order doesn't matter you might want to split the assertion to two groups.",
				`{"level":"info","message":"Opened file", "file":"file5"}`,
			),
		},
		{
			name:     "match not scalar fields",
			expected: `{"level":"info","message":"Info msg","array":["foo","bar"],"object":{"foo":"bar"}}`,
			actual:   `{"level":"info","message":"Info msg","array":["foo","bar"],"object":{"foo":"bar"}}`,
		},
		{
			name:     "match not scalar fields - fail",
			expected: `{"level":"info","message":"Info msg","array":["123","456"],"object":{"foo":"bar"}}`,
			actual:   `{"level":"info","message":"Info msg","array":["foo","bar"],"object":{"foo":"bar"}}`,
			err: errors.Errorf(
				"Expected:\n-----\n%s\n-----\nActual:\n-----\n%s",
				`{"level":"info","message":"Info msg","array":["123","456"],"object":{"foo":"bar"}}`,
				`{"level":"info","message":"Info msg","array":["foo","bar"],"object":{"foo":"bar"}}`,
			),
		},
	}

	for _, c := range cases {
		err := CompareJSONMessages(c.expected, c.actual)

		if c.err == nil {
			require.NoError(t, err, c.name)
		} else {
			require.Error(t, err, c.name)
			assert.Equal(t, c.err.Error(), err.Error(), c.name)
		}
	}
}

func TestAssertWildcardsDifferent1(t *testing.T) {
	t.Parallel()
	test := &mockedT{buf: bytes.NewBuffer(nil)}
	ok := AssertJSONMessages(
		test,
		`
{"level":"info","message":"Opened file", "file":"file2"}
{"level":"info","message":"Opened file", "file":"file4"}
{"level":"info","message":"Opened file", "file":"file6"}
`,
		`
{"level":"info","message":"Opened file", "file":"file1"}
{"level":"info","message":"Opened file", "file":"file2"}
{"level":"info","message":"Opened file", "file":"file3"}
{"level":"info","message":"Opened file", "file":"file4"}
{"level":"info","message":"Opened file", "file":"file5"}
{"level":"info","message":"Opened file", "file":"file7"}
`,
	)
	assert.False(t, ok)
	expected := `
Expected:
-----
{"level":"info","message":"Opened file", "file":"file6"}
-----
Actual:
-----
{"level":"info","message":"Opened file", "file":"file5"}
{"level":"info","message":"Opened file", "file":"file7"}
`
	// Get error message
	_, testLog, _ := strings.Cut(test.buf.String(), "Error:")
	// Trim leading whitespaces from each line
	testLog = regexp.MustCompile(`(?m)^\s+`).ReplaceAllString(testLog, "")
	// Compare
	assert.Equal(t, strings.TrimSpace(expected), strings.TrimSpace(testLog))
}
