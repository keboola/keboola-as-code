//go:build windows

package terminal

import (
	"testing"

	"github.com/Netflix/go-expect"
)

func New(t *testing.T, opts ...expect.ConsoleOpt) (Console, error) {
	t.Helper()
	t.Skipf(`virtual terminal in not supported in Windows tests`)
	return nil, nil
}
