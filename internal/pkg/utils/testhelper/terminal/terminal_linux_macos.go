//go:build !windows

package terminal

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/ActiveState/vt10x"
	"github.com/Netflix/go-expect"
	"github.com/acarl005/stripansi"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
)

const (
	termEscChar   = '\x1b'
	sendDelay     = 20 * time.Millisecond
	expectTimeout = 15 * time.Second
)

// console implements Console.
type console struct {
	*expect.Console
	state *vt10x.State
	tty   *tty
}

// ansiSplitReader is workaround for the issue in the vtx10/survey libraries.
// There is an edge-case, when an <stdin> message and some <ansi> terminal escape sequence are read together: "<stdin><ansi>".
// The "survey" library does not recognize <ansi> expression and waits endlessly.
// It only happens in tests, in reality such a situation probably cannot happen.
// We do not know if it is a bug in the vtx10 terminal emulator or in the survey CLI library.
// Workaround: Split "<stdin><ansi>", to "<stdin>", "<ansi>" on read, using the Scanner.
type ansiSplitReader struct {
	scanner *bufio.Scanner
}

func newAnsiSplitReader(in io.Reader) io.Reader {
	r := &ansiSplitReader{}
	r.scanner = bufio.NewScanner(in)
	r.scanner.Split(func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		// Return the part up to the first ANSI escape sequence
		if i := bytes.IndexByte(data, termEscChar); i >= 1 {
			return i, data[0:i], nil
		}
		// No ANSI escape sequence, return all
		return len(data), data, nil
	})
	return r
}

func (r *ansiSplitReader) Read(b []byte) (int, error) {
	if !r.scanner.Scan() {
		return 0, io.EOF
	}
	if err := r.scanner.Err(); err != nil {
		return 0, err
	}
	s := r.scanner.Bytes()
	if len(b) < len(s) {
		panic(errors.Errorf("small buffer %d, required %d", len(b), len(s)))
	}
	copy(b, s)
	return len(s), nil
}

// tty implements Tty, it is an os.File wrapper for virtual terminal TTY.
// Unlike os.File, when Close() is called,
// all running Read and Write operations are immediately terminated,
// so the test timeout does not occur.
type tty struct {
	file   *os.File
	reader io.Reader
	closed chan struct{}
}

func New(t *testing.T, opts ...expect.ConsoleOpt) (Console, error) {
	t.Helper()

	out := &console{}
	var err error

	// Log console output to stdout, if TEST_VERBOSE=true
	debugStdout := testhelper.VerboseStdout()
	opts = append(
		opts,
		expect.WithStdout(debugStdout),
		expect.WithCloser(debugStdout),
		expect.WithSendObserver(sendObserver(t, debugStdout)),
		expect.WithExpectObserver(expectObserver(t, os.Stderr)), // nolint:forbidigo
		expect.WithDefaultTimeout(expectTimeout),
	)

	out.Console, out.state, err = vt10x.NewVT10XConsole(opts...)

	ttyFile := out.Console.Tty()
	out.tty = &tty{file: ttyFile, reader: newAnsiSplitReader(ttyFile), closed: make(chan struct{})}
	return out, err
}

func (c *console) Tty() Tty {
	return c.tty
}

func (c *console) TtyRaw() *os.File {
	return c.Console.Tty()
}

func (c *console) String() string {
	return c.state.String()
}

func (c *console) Send(s string) error {
	c.waitBeforeSend()
	_, err := c.Console.Send(s)
	return err
}

func (c *console) SendLine(s string) error {
	c.waitBeforeSend()
	_, err := c.Console.SendLine(s)
	return err
}

func (c *console) SendEnter() error {
	return c.Send("\n")
}

func (c *console) SendSpace() error {
	return c.Send(" ")
}

func (c *console) SendBackspace() error {
	return c.Send("\u0008")
}

func (c *console) SendUpArrow() error {
	return c.Send("\u001B[A")
}

func (c *console) SendDownArrow() error {
	return c.Send("\u001B[B")
}

func (c *console) SendRightArrow() error {
	return c.Send("\u001B[C")
}

func (c *console) SendLeftArrow() error {
	return c.Send("\u001B[D")
}

func (c *console) ExpectString(s string, opts ...expect.ExpectOpt) error {
	opts = append(opts, StringWithoutANSI(s))
	_, err := c.Console.Expect(opts...)
	return err
}

func (c *console) ExpectEOF(opts ...expect.ExpectOpt) (err error) {
	defer func() {
		// Close STDIN on error (e.g. timeout)
		if err != nil {
			_ = c.Tty().Close()
		}
	}()

	// Better error message
	opts = append(opts, expect.EOF, expect.PTSClosed)
	if _, err := c.Console.Expect(opts...); err != nil {
		return errors.Errorf("error while waiting for EOF: %w", err)
	} else {
		return nil
	}
}

// waitBeforeSend delays sending input.
// We often Expect only the part of the output that is written to the console.
// But we can send the next input only when the application expects it,
// so all application output has to be written before.
// The simplest solution is to wait a bit.
func (c *console) waitBeforeSend() {
	time.Sleep(sendDelay)
}

func (t *tty) Read(p []byte) (int, error) {
	// Within the tests, interactive/Prompt.Editor is called.
	// It starts a new OS process for the editor (to edit a longer value).
	// In the tests, instead of the editor (vi, nano, ...), the command "true" is started, which ends immediately.
	//
	// The os/exec package works differently with stdin, depending on whether it is *os.File or another io.Reader.
	// See: https://github.com/golang/go/blob/d7df872267f9071e678732f9469824d629cac595/src/os/exec/exec.go#L350-L357
	// Our *tty implementation implements io.Reader, it is not an instance of *os.File.
	//
	// In os/exec, io.Copy function is called for io.Reader to redirect reader -> pipe.
	// This blocks the test by unexpected reading from stdin, which does not happen during real execution (*os.File).
	// For this reason, this call is terminated immediately.
	if calledFromOsExecCmdStart() {
		return 0, io.EOF
	}

	var n int
	var err error
	done := make(chan struct{})

	go func() {
		n, err = t.reader.Read(p)
		close(done)
	}()

	select {
	case <-t.closed:
		return 0, errors.New("cannot read: tty closed")
	case <-done:
		return n, err
	}
}

func (t *tty) Write(p []byte) (int, error) {
	var n int
	var err error
	done := make(chan struct{})

	go func() {
		n, err = t.file.Write(p)
		close(done)
	}()

	select {
	case <-t.closed:
		return 0, errors.New("cannot write: tty closed")
	case <-done:
		return n, err
	}
}

func (t *tty) Fd() uintptr {
	return t.file.Fd()
}

func (t *tty) Close() error {
	select {
	case <-t.closed:
		return errors.New("tty already closed")
	default:
		close(t.closed)
		return t.file.Close()
	}
}

func sendObserver(t *testing.T, writer io.Writer) expect.SendObserver {
	t.Helper()
	return func(msg string, num int, err error) {
		t.Helper()
		if err == nil {
			_, _ = fmt.Fprintf(writer, "\n\n>>> SEND: %+q\n\n", msg)
		} else {
			_, _ = fmt.Fprintf(writer, "\n\n>>> SEND %+q ERROR: %s\n\n", msg, err)
		}
	}
}

func expectObserver(t *testing.T, writer io.Writer) expect.ExpectObserver {
	t.Helper()
	return func(matchers []expect.Matcher, buf string, err error) {
		t.Helper()
		if err != nil {
			var criteria []any
			for _, m := range matchers {
				criteria = append(criteria, m.Criteria())
			}

			_, _ = fmt.Fprintf(
				writer,
				"\n\n>>> Could not meet expectations %v, error: %v\nTerminal snapshot:\n-----\n%s\n-----\n",
				criteria, err, stripansi.Strip(buf),
			)
		}
	}
}

func calledFromOsExecCmdStart() bool {
	// Get last 20 call stack frames
	rpc := make([]uintptr, 20)
	runtime.Callers(0, rpc)
	frames := runtime.CallersFrames(rpc)

	// Search form os/exec.(*Cmd).Start method
	for {
		frame, more := frames.Next()
		if strings.Contains(frame.Function, "os/exec.(*Cmd).Start") {
			return true
		}
		if !more {
			break
		}
	}
	return false
}
