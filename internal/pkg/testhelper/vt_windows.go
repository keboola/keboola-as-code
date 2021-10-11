//go:build windows
// +build windows

package testhelper

import (
	"testing"

	"github.com/ActiveState/vt10x"
	"github.com/Netflix/go-expect"
)

func NewVirtualTerminal(t *testing.T, opts ...expect.ConsoleOpt) (*expect.Console, *vt10x.State, error) {
	t.Helper()
	t.Skipf(`virtual terminal in not supported in Windows tests`)
	return nil, nil, nil
}
