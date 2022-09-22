package terminal

import (
	"io"
	"time"

	"github.com/AlecAivazis/survey/v2/terminal"
)

const sendDelay = 20 * time.Millisecond
const expectTimeout = 5 * time.Second

// Console is virtual terminal for tests.
type Console interface {
	// Tty returns Console's pts (slave part of a pty). A pseudoterminal, or pty is
	// a pair of pseudo-devices, one of which, the slave, emulates a real text
	// terminal device.
	Tty() Tty
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
	ExpectString(s string) error
	// ExpectEOF reads from Console's tty until EOF or an error occurs, and returns
	// the buffer read by Console.  We also treat the PTSClosed error as an EOF.
	ExpectEOF() error
	// Close closes both the TTY and afterwards all the readers
	Close() error
}

// Tty provides reader (stdin) and writer (stdout/stderr) for virtual terminal.
type Tty interface {
	terminal.FileReader
	terminal.FileWriter
	io.Closer
}
