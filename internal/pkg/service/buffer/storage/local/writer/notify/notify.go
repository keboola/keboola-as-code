package notify

import (
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// Notifier allows multiple listeners to Wait for the completion of an operation that may end with an error.
// It is used while awaiting synchronization, see the disksync package.
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
func (n *Notifier) Wait() error {
	if n == nil {
		return nil
	}

	<-n.doneCh
	return n.error
}
