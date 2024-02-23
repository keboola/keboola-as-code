package etcdlogger_test

import (
	"bytes"
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdlogger"
)

type mockedT struct {
	buf *bytes.Buffer
}

// Implements TestingT for mockedT.
func (t *mockedT) Errorf(format string, args ...any) {
	s := fmt.Sprintf(format, args...)
	t.buf.WriteString(s)
}

func TestAssert(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		error    string
		expected string
		actual   string
	}{
		// -------------------------------------------------------------------------------------------------------------
		{
			name:     "empty",
			actual:   "",
			expected: "",
		},

		// -------------------------------------------------------------------------------------------------------------
		{
			name: "complex - ok",
			expected: `
// Comment 1
➡️  TXN
  ➡️  IF:
  // Added lines



  001 "key/1" MOD GREATER 0
  
  002 "key/1" MOD LESS %d
  ➡️  THEN:
  001 ➡️  PUT "foo"
// Removed lines
✔️  TXN | succeeded: false
`,
			actual: `
➡️  TXN
  ➡️  IF:
  001 "key/1" MOD GREATER 0
  002 "key/1" MOD LESS 123456
  ➡️  THEN:
  001 ➡️  PUT "foo"


✔️  TXN | succeeded: false
`,
		},
		// -------------------------------------------------------------------------------------------------------------
		{
			name: "complex - error",
			error: `
Diff:
-----
@@ -3 +3 @@
-␣␣002␣"key/1"␣MOD␣LESS␣%d
+␣␣001␣"key/1"␣MOD␣GREATER␣0
@@ -5 +5 @@
-␣␣001␣➡️␣␣PUT␣"foo"
+␣␣001␣➡️␣␣PUT␣"bar"
-----
Actual:
-----
➡️  TXN
  ➡️  IF:
  001 "key/1" MOD GREATER 0
  ➡️  THEN:
  001 ➡️  PUT "bar"
✔️  TXN | succeeded: false
-----
Expected:
-----
➡️  TXN
  ➡️  IF:
  002 "key/1" MOD LESS %d
  ➡️  THEN:
  001 ➡️  PUT "foo"
✔️  TXN | succeeded: false
-----
`,
			expected: `
// Comment 1
➡️  TXN
  ➡️  IF:
  // Added lines



  002 "key/1" MOD LESS %d
  ➡️  THEN:
  001 ➡️  PUT "foo"
// Removed lines
✔️  TXN | succeeded: false
`,
			actual: `
➡️  TXN
  ➡️  IF:
  001 "key/1" MOD GREATER 0
  ➡️  THEN:
  001 ➡️  PUT "bar"


✔️  TXN | succeeded: false
`,
			// -------------------------------------------------------------------------------------------------------------
		},
	}

	for _, c := range cases {
		test := &mockedT{buf: bytes.NewBuffer(nil)}

		// Run assert
		etcdlogger.Assert(test, c.expected, c.actual)

		// Get error message
		_, testLog, _ := strings.Cut(test.buf.String(), "Error:")

		// Trim leading whitespaces from each line
		testLog = strings.TrimSpace(regexp.MustCompile(`(?m)^\s{14}`).ReplaceAllString(testLog, ""))

		// Compare
		if c.error == "" {
			assert.Empty(t, testLog, c.name)
		} else {
			assert.Equal(t, strings.TrimSpace(c.error), testLog, c.name)
		}
	}
}
