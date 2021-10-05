package testhelper

import (
	"io"

	"github.com/ActiveState/vt10x"
	"github.com/Netflix/go-expect"
)

func NewVirtualTerminal(opts ...expect.ConsoleOpt) (*expect.Console, *vt10x.State, error) {
	r, w := io.Pipe() // simulate pty
	var state vt10x.State
	term, err := vt10x.New(&state, r, w)
	if err != nil {
		return nil, nil, err
	}

	c, err := expect.NewConsole(append(opts, expect.WithStdin(r), expect.WithStdout(term), expect.WithCloser(term))...)
	if err != nil {
		return nil, nil, err
	}

	return c, &state, nil
}
