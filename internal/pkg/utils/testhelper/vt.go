//go:build !windows && !darwin
// +build !windows,!darwin

package testhelper

import (
	"testing"

	"github.com/ActiveState/vt10x"
	"github.com/Netflix/go-expect"
)

func NewVirtualTerminal(t *testing.T, opts ...expect.ConsoleOpt) (*expect.Console, *vt10x.State, error) {
	t.Helper()
	return vt10x.NewVT10XConsole(opts...)
}
