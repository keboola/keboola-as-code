package notify

import (
	"context"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// Notifier allows multiple listeners to Wait for the completion of an operation that may end with an error.
// It is used while awaiting synchronization, see the writesync package.
type Notifier struct {
	doneCh chan struct{}
	error  error
}

func New() *Notifier {
	return &Notifier{doneCh: make(chan struct{})}
}

func (n *Notifier) Done(err error) {
	if n == nil {
		panic(errors.New("notifier is not initialized"))
	}

	n.error = err
	close(n.doneCh)
}

// Wait for the operation.
func (n *Notifier) Wait(ctx context.Context) error {
	// *notify.Notifier(nil).Wait() is valid call
	if n == nil {
		return nil
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-n.doneCh:
		return n.error
	}
}

// WaitWithTimeout for the operation.
func (n *Notifier) WaitWithTimeout(timeout time.Duration) error {
	// *notify.Notifier(nil).WaitWithTimeout(...) is valid call
	if n == nil {
		return nil
	}

	select {
	case <-n.doneCh:
	case <-time.After(timeout):
		return errors.Errorf(`timeout after %s`, timeout)
	}

	return n.error
}
