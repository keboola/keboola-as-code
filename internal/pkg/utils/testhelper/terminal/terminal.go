package terminal

import (
	"bytes"
	"io"
	"os"
	"strings"

	"github.com/AlecAivazis/survey/v2/terminal"
	"github.com/Netflix/go-expect"
	"github.com/acarl005/stripansi"
)

// Console is virtual terminal for tests.
type Console interface {
	// Tty returns Console's reader/writer, it should be used in unit tests.
	Tty() Tty
	// TtyRaw returns Console's pts (slave part of a pty), it should be used to run an OS command.
	TtyRaw() *os.File
	// String returns a string representation of the terminal output.
	String() string
	// Send writes string s to Console's tty.
	Send(s string) error
	// SendLine writes string s to Console's tty.
	SendLine(s string) error
	SendEnter() error
	SendSpace() error
	SendBackspace() error
	SendUpArrow() error
	SendDownArrow() error
	SendRightArrow() error
	SendLeftArrow() error
	// ExpectString reads from Console's tty until the provided string is read or
	// an error occurs, and returns the buffer read by Console.
	ExpectString(s string, opts ...expect.ExpectOpt) error
	// ExpectEOF reads from Console's tty until EOF or an error occurs, and returns
	// the buffer read by Console.  We also treat the PTSClosed error as an EOF.
	ExpectEOF(opts ...expect.ExpectOpt) error
	// Close closes both the TTY and afterwards all the readers
	Close() error
}

// Tty provides reader (stdin) and writer (stdout/stderr) for virtual terminal.
type Tty interface {
	terminal.FileReader
	terminal.FileWriter
	io.Closer
}

// stringWithoutANSIMatcher fulfills the Matcher interface to match strings.
// ANSI escape characters are ignored.
type stringWithoutANSIMatcher struct {
	str string
}

func (m *stringWithoutANSIMatcher) Match(v interface{}) bool {
	buf, ok := v.(*bytes.Buffer)
	if !ok {
		return false
	}
	if strings.Contains(stripansi.Strip(buf.String()), m.str) {
		return true
	}
	return false
}

func (m *stringWithoutANSIMatcher) Criteria() interface{} {
	return m.str
}

// StringWithoutANSI adds an Expect condition to exit if the content read from Console's
// tty contains any of the given strings.
// ANSI escape characters are ignored.
func StringWithoutANSI(strs ...string) expect.ExpectOpt {
	return func(opts *expect.ExpectOpts) error {
		for _, str := range strs {
			opts.Matchers = append(opts.Matchers, &stringWithoutANSIMatcher{
				str: str,
			})
		}
		return nil
	}
}
