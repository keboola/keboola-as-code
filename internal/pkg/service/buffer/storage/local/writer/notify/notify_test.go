package notify

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/ioutil"
)

const (
	testWaitTimeout = 2 * time.Second
)

func TestNotifier_Nil(t *testing.T) {
	t.Parallel()

	var n *Notifier

	// Notifier can be used as a nil value, Wait ends immediately, without error
	assert.NoError(t, n.Wait())
	assert.NoError(t, n.WaitWithTimeout(testWaitTimeout))

	// But call of the Done fails
	assert.Panics(t, func() {
		n.Done(nil)
	})
}

func TestNotifier_WaitWithTimeout(t *testing.T) {
	t.Parallel()

	err := New().WaitWithTimeout(time.Millisecond)
	if assert.Error(t, err) {
		assert.Equal(t, "timeout after 1ms", err.Error())
	}
}

func TestNotifier_Success(t *testing.T) {
	t.Parallel()

	log := ioutil.NewAtomicWriter()
	wg := &sync.WaitGroup{}

	n := New()
	wait := func() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			assert.NoError(t, n.WaitWithTimeout(testWaitTimeout))
			assert.NoError(t, n.Wait())
			_, _ = log.WriteString("wait finished\n")
		}()
	}

	// Call Wait 5x before done
	for i := 0; i < 5; i++ {
		wait()
	}

	// Call done, mark the operation finished without error
	time.Sleep(10 * time.Millisecond)
	_, _ = log.WriteString("done\n")
	n.Done(nil)

	// Call Wait 5x after done
	for i := 0; i < 5; i++ {
		wait()
	}

	// Wait for goroutines
	wg.Wait()

	assert.Equal(t, strings.TrimSpace(`
done
wait finished
wait finished
wait finished
wait finished
wait finished
wait finished
wait finished
wait finished
wait finished
wait finished
`), strings.TrimSpace(log.String()))
}

func TestNotifier_Error(t *testing.T) {
	t.Parallel()

	log := ioutil.NewAtomicWriter()
	wg := &sync.WaitGroup{}

	n := New()
	wait := func() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := n.WaitWithTimeout(testWaitTimeout)
			if assert.Error(t, err) {
				assert.Equal(t, "some error", err.Error())
			}
			_, _ = log.WriteString("wait finished\n")
		}()
	}

	// Call Wait 5x before done
	for i := 0; i < 5; i++ {
		wait()
	}

	// Call done, mark the operation finished without error
	time.Sleep(10 * time.Millisecond)
	_, _ = log.WriteString("done\n")
	n.Done(errors.New("some error"))

	// Call Wait 5x after done
	for i := 0; i < 5; i++ {
		wait()
	}

	// Wait for goroutines
	wg.Wait()

	assert.Equal(t, strings.TrimSpace(`
done
wait finished
wait finished
wait finished
wait finished
wait finished
wait finished
wait finished
wait finished
wait finished
wait finished
`), strings.TrimSpace(log.String()))
}
