package etcdop

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	etcd "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	sessionDefaultGrantTimeout = 5 * time.Second
	sessionDefaultTTLSeconds   = 15
)

// Session wraps an etcd session with retries.
// If there is a longer network outage and the session expires, then a new session is created.
//
// Each session creation is reported via the OnSession callback.
// The callback must not be blocking.
// In the work you start in the OnSession callback, you should check <-session.Done().
//
// The Session function waits for:
// - The first session creation.
// - The first keep-alive request.
// - The completion of the OnSession callbacks call.
//
// Any initialization error is reported via the error channel.
// After successful initialization, a new session is created after each failure until the context ends.
type Session struct {
	sessionBuilder SessionBuilder
	logger         log.Logger
	client         *etcd.Client
	lessor         etcd.Lease
	backoff        *backoff.ExponentialBackOff

	mutexStore *mutexStore

	lock    *sync.Mutex
	created chan struct{} // see WaitForSession
	actual  *concurrency.Session
}

type SessionBuilder struct {
	grantTimeout time.Duration
	ttlSeconds   int
	onSession    []onSession
}

type onSession func(session *concurrency.Session) error

type NoSessionError struct{}

func (e NoSessionError) Error() string {
	return "no active concurrent.Session"
}

// NewSessionBuilder creates a builder for the resistant Session.
func NewSessionBuilder() *SessionBuilder {
	return &SessionBuilder{
		grantTimeout: sessionDefaultGrantTimeout,
		ttlSeconds:   sessionDefaultTTLSeconds,
	}
}

// WithGrantTimeout configures the maximum time to wait for creating a new session.
func (b SessionBuilder) WithGrantTimeout(v time.Duration) SessionBuilder {
	if v <= 0 {
		panic(errors.New("grant timeout must be > 0"))
	}
	b.grantTimeout = v
	return b
}

// WithTTLSeconds configures the session's TTL in seconds.
func (b SessionBuilder) WithTTLSeconds(v int) SessionBuilder {
	if v <= 0 {
		panic(errors.New("TTL must be > 0"))
	}
	b.ttlSeconds = v
	return b
}

// WithOnSession registers a callback that is called on each session creation.
// The callback must not be blocking.
// In the work you start in the OnSession callback, you should check <-session.Done().
func (b SessionBuilder) WithOnSession(fn onSession) SessionBuilder {
	b.onSession = append([]onSession(nil), b.onSession...)
	b.onSession = append(b.onSession, fn)
	return b
}

// StartOrErr the resistant Session.
// Any initialization error is returned.
// After successful initialization, a new session is created after each failure until the context ends.
func (b SessionBuilder) StartOrErr(ctx context.Context, wg *sync.WaitGroup, logger log.Logger, client *etcd.Client) (*Session, error) {
	sess, errCh := b.Start(ctx, wg, logger, client)
	if err := <-errCh; err != nil {
		return nil, err
	}
	return sess, nil
}

// Start the resistant Session.
// Any initialization error is reported via the error channel.
// After successful initialization, a new session is created after each failure until the context ends.
func (b SessionBuilder) Start(ctx context.Context, wg *sync.WaitGroup, logger log.Logger, client *etcd.Client) (*Session, <-chan error) {
	initDoneOut := make(chan error, 1)
	s := &Session{
		sessionBuilder: b,
		logger:         logger.WithComponent("etcd.session"),
		client:         client,
		lessor:         etcd.NewLease(client),
		backoff:        newSessionBackoff(),
		lock:           &sync.Mutex{},
		created:        make(chan struct{}),
	}

	s.mutexStore = newMutexStore(s)

	wg.Add(1)
	go func() {
		defer wg.Done()
		initDone := initDoneOut

		for {
			// Check context
			if err := ctx.Err(); err != nil {
				_ = s.closeSession(ctx, err.Error())
				return
			}

			// Create session
			s.logger.Infof(ctx, `creating etcd session`)
			session, err := s.newSession(ctx)

			// Ok, replace the reference
			if err == nil {
				s.lock.Lock()
				created := s.created
				s.created = make(chan struct{})
				s.actual = session
				s.lock.Unlock()
				close(created) // notify WaitForSession
			}

			// Notify about initialization, the first iteration
			if initDone != nil {
				if err == nil {
					// Initialization has been successful
					close(initDone)
					initDone = nil
				} else {
					// Notify and stop on an initialization error
					initDone <- err
					close(initDone)
					return
				}
			}

			// Retry on error
			if err != nil {
				s.logger.Infof(ctx, "cannot create etcd session: %s", err.Error())
				if !errors.Is(err, context.Canceled) {
					fmt.Println("context canceled, removing session:", err.Error())
					if errors.Is(err, context.DeadlineExceeded) {
						fmt.Println("context deadline exceeded, removing session:", err.Error())
						// Do not check error from closeSession, it can be already closed
						_ = s.closeSession(ctx, "etcd unreachable, remove existing sessions")
						fmt.Println("context deadline exceeded, removed session")
					}

					delay := s.backoff.NextBackOff()
					s.logger.Infof(ctx, "waiting %s before the retry", delay)
					select {
					case <-ctx.Done():
					case <-time.After(delay):
					}
				}
				continue // retry
			}

			// Wait for context or session cancellation
			select {
			case <-ctx.Done():
			case <-session.Done():
				s.logger.Info(ctx, "etcd session canceled")
			}
		}
	}()

	return s, initDoneOut
}

// Session returns active session or NoSessionError on a network outage.
func (s *Session) Session() (*concurrency.Session, error) {
	s.lock.Lock()
	session := s.actual
	s.lock.Unlock()

	select {
	case <-session.Done():
		// there is no active session during an outage
		return nil, NoSessionError{}
	default:
		// ok
		return session, nil
	}
}

// WaitForSession returns active session or waits for a new session on a network outage.
// The error is returned only if the context is cancelled.
func (s *Session) WaitForSession(ctx context.Context) (*concurrency.Session, error) {
	for {
		s.lock.Lock()
		session := s.actual
		ready := s.created
		s.lock.Unlock()

		select {
		case <-ctx.Done():
			// stop on the context cancellation
			return nil, ctx.Err()
		case <-session.Done():
			// wait for the session re-creation
			fmt.Println("waiting for the session re-creation")
			<-ready
			continue
		default:
			// ok
			fmt.Println("session is ready and ok")
			return session, nil
		}
	}
}

// NewMutex returns *concurrency.Mutex that implements the sync Locker interface with etcd.
// IsOwner().
func (s *Session) NewMutex(name string) *Mutex {
	return s.mutexStore.NewMutex(name)
}

// newSession underlying low-level *concurrency.Session.
func (s *Session) newSession(ctx context.Context) (_ *concurrency.Session, err error) {
	startTime := time.Now()

	// Obtain the LeaseID
	// The concurrency.NewSession bellow can do it by itself, but we need a separate context with a timeout here.
	grantCtx, grantCancel := context.WithTimeoutCause(ctx, s.sessionBuilder.grantTimeout, errors.New("session grant timeout"))
	defer grantCancel()
	grantResp, err := s.lessor.Grant(grantCtx, int64(s.sessionBuilder.ttlSeconds))
	if err != nil {
		return nil, err
	}

	// Create session
	session, err := concurrency.NewSession(s.client, concurrency.WithTTL(s.sessionBuilder.ttlSeconds), concurrency.WithLease(grantResp.ID))
	if err != nil {
		return nil, err
	}

	// Close session on an error bellow
	defer func() {
		if err != nil {
			_ = session.Close()
		}
	}()

	// Check connection, wait for the first keep-alive.
	// It prevents weird warnings if a test ends before the first keep alive is completed.
	if _, err = s.lessor.KeepAliveOnce(ctx, session.Lease()); err != nil {
		return nil, err
	}

	// Invoke callbacks - start session dependent work
	s.logger.WithDuration(time.Since(startTime)).Infof(ctx, "created etcd session")
	for i, fn := range s.sessionBuilder.onSession {
		if err := fn(session); err != nil {
			err = errors.Errorf(`callback OnSession[%d] failed: %s`, i, err)
			s.logger.Error(ctx, err.Error())
			return nil, err
		}
	}

	// Reset retry backoff
	s.backoff.Reset()

	return session, nil
}

// closeSession closes underlying low-level *concurrency.Session.
func (s *Session) closeSession(ctx context.Context, reason string) error {
	// Get session
	session, err := s.Session()
	if session == nil || err != nil {
		return err // the session is already closed or expired
	}

	// Close session
	startTime := time.Now()
	if reason == "" {
		s.logger.Info(ctx, "closing etcd session")
	} else {
		s.logger.Infof(ctx, "closing etcd session: %s", reason)
	}
	if err := session.Close(); err != nil {
		err = errors.PrefixError(err, "cannot close etcd session")
		s.logger.Warnf(ctx, err.Error())
		return err
	}

	// Close lease client, keep alive, ...
	if err := s.lessor.Close(); err != nil {
		err = errors.PrefixError(err, "cannot close etcd session lessor")
		s.logger.Warnf(ctx, err.Error())
		return err
	}

	// Ok
	s.logger.WithDuration(time.Since(startTime)).Info(ctx, "closed etcd session")
	return nil
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
