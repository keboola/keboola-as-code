package local

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func TestWorkers(t *testing.T) {
	t.Parallel()
	w := NewWorkers(context.Background())

	counter := utils.SafeCounter{}
	w.AddWorker(func() error {
		counter.Inc()
		return nil
	})
	w.AddWorker(func() error {
		counter.Inc()
		return nil
	})

	// Not stared
	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, 0, counter.Get())

	// Start and wait
	assert.NoError(t, w.StartAndWait())
	assert.Equal(t, 2, counter.Get())

	// Cannot be reused
	assert.PanicsWithError(t, `invoked local.Workers cannot be reused`, func() {
		w.StartAndWait()
	})
}

func TestWorkersErrors(t *testing.T) {
	t.Parallel()
	w := NewWorkers(context.Background())

	w.AddWorker(func() error {
		return fmt.Errorf(`first`)
	})
	w.AddWorker(func() error {
		return fmt.Errorf(`second`)
	})
	w.AddWorker(func() error {
		return nil
	})
	w.AddWorker(func() error {
		return fmt.Errorf(`third`)
	})
	w.AddWorker(func() error {
		return nil
	})

	// The order of errors is the same as the workers were defined
	err := w.StartAndWait()
	assert.Error(t, err)
	assert.Equal(t, "- first\n- second\n- third", err.Error())
}
