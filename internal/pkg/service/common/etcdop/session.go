package etcdop

import (
	"context"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	etcd "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

// ResistantSession creates an etcd session with retries.
// If there is a longer network outage and the session expires, then a new session is created.
//
// Each session creation is reported via the onSession callback.
// The callback must not be blocking.
//
// In the work you start in the onSession callback, you should check <-session.Done().
//
// The ResistantSession function waits for:
// - The first session creation.
// - The completion of the first OnSession callback call.
//
// Any initialization error is reported via the error channel.
// After successful initialization, a new session is created after each failure until the context ends.
func ResistantSession(ctx context.Context, wg *sync.WaitGroup, logger log.Logger, client *etcd.Client, ttlSeconds int, onSession func(session *concurrency.Session) error) <-chan error {
	b := newSessionBackoff()
	startTime := time.Now()
	logger = logger.AddPrefix("[etcd-session]")
	logger.Infof(`creating etcd session`)

	wg.Add(1)
	initDone := make(chan error, 1)
	initDoneOut := initDone
	go func() {
		defer wg.Done()
		for {
			// Wait before re-creation attempt, except the initialization
			if initDone == nil {
				delay := b.NextBackOff()
				logger.Infof("re-creating etcd session, backoff delay %s", delay)
				<-time.After(delay)
			}

			// Create session
			session, err := concurrency.NewSession(client, concurrency.WithTTL(ttlSeconds))
			if err != nil {
				if initDone == nil {
					// Try again
					logger.Errorf(`cannot create etcd session: %s`, err)
					continue
				} else {
					// Stop initialization
					initDone <- err
					close(initDone)
					return
				}
			}

			// Reset session backoff
			b.Reset()
			logger.Infof("created etcd session | %s", time.Since(startTime))

			// Start session dependent work
			err = onSession(session)
			if err != nil {
				if initDone == nil {
					logger.Errorf(`etcd session callback failed: %s`, err)
				} else {
					// Stop initialization
					initDone <- err
					close(initDone)
					return
				}
			}

			// Mark initialization done
			if initDone != nil {
				close(initDone)
				initDone = nil
			}

			// Check ctx and session
			select {
			case <-ctx.Done():
				// Context cancelled
				startTime := time.Now()
				logger.Info("closing etcd session")
				if err := session.Close(); err != nil {
					logger.Warnf("cannot close etcd session: %s", err)
				} else {
					logger.Infof("closed etcd session | %s", time.Since(startTime))
				}
				return
			case <-session.Done():
				// Re-create ...
			}
		}
	}()

	return initDoneOut
}

func newSessionBackoff() *backoff.ExponentialBackOff {
	b := backoff.NewExponentialBackOff()
	b.RandomizationFactor = 0.2
	b.InitialInterval = 50 * time.Millisecond
	b.Multiplier = 2
	b.MaxInterval = 1 * time.Minute
	b.MaxElapsedTime = 0 // never stop
	b.Reset()
	return b
}
