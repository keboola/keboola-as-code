//go:build darwin
// +build darwin

package terminal

import (
	"testing"

	"github.com/Netflix/go-expect"
)

func New(t *testing.T, opts ...expect.ConsoleOpt) (Console, error) {
	t.Helper()
	t.Skipf(`virtual terminal is not stable in Mac Os tests`)
	return nil, nil
}
