//go:build darwin
// +build darwin

package testhelper

import (
	"testing"

	"github.com/ActiveState/vt10x"
	"github.com/Netflix/go-expect"
)

func NewVirtualTerminal(t *testing.T, opts ...expect.ConsoleOpt) (*expect.Console, *vt10x.State, error) {
	t.Helper()
	t.Skipf(`virtual terminal is not stable in Mac Os tests`)
	return nil, nil, nil
}
