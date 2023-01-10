package distribution

import (
	"context"
	"sync"

	"github.com/benbjohnson/clock"
	gonanoid "github.com/matoous/go-nanoid/v2"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
)

// Listener contains channel C with distribution change Events.
type Listener struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     *sync.WaitGroup
	all    *listeners
	id     listenerID
	C      chan Events
}

type listeners struct {
	lock           *sync.Mutex
	bufferedEvents []Event
	listeners      map[listenerID]*Listener
}

type listenerID string

func newListeners(proc *servicectx.Process, clock clock.Clock, logger log.Logger, config config) *listeners {
	logger = logger.AddPrefix("[listeners]")
	v := &listeners{
		lock:      &sync.Mutex{},
		listeners: make(map[listenerID]*Listener),
	}

	// Graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	proc.OnShutdown(func() {
		logger.Info("received shutdown request")
		cancel()
		wg.Wait()
		logger.Info("shutdown done")
	})

	wg.Add(1)
	go func() {
		defer wg.Done()

		// Listeners are not triggered immediately on change,
		// but all events within the groupInterval are processed at once.
		triggerTicker := clock.Ticker(config.eventsGroupInterval)
		defer triggerTicker.Stop()

		for {
			select {
			case <-ctx.Done():
				// Handle shutdown
				logger.Info("waiting for listeners")
				v.lock.Lock()
				// Process remaining events
				v.trigger()
				// Stop all listeners
				for _, l := range v.listeners {
					l.wg.Wait()
					l.cancel()
					close(l.C)
				}
				v.listeners = nil
				v.lock.Unlock()
				return
			case <-triggerTicker.C:
				// Trigger listeners at most once per "group interval"
				v.lock.Lock()
				v.trigger()
				v.lock.Unlock()
			}
		}
	}()

	return v
}

// Notify listeners about a new event. The event is not processed immediately.
// All events within the "group interval" are processed at once, see trigger method.
func (v *listeners) Notify(event Event) {
	v.lock.Lock()
	v.bufferedEvents = append(v.bufferedEvents, event)
	v.lock.Unlock()
}

// add a new listener, it contains channel C with streamed distribution change Events.
func (v *listeners) add() *Listener {
	ctx, cancel := context.WithCancel(context.Background())
	out := &Listener{
		ctx:    ctx,
		cancel: cancel,
		wg:     &sync.WaitGroup{},
		all:    v,
		id:     listenerID(gonanoid.Must(10)),
		C:      make(chan Events),
	}
	v.lock.Lock()
	v.listeners[out.id] = out
	v.lock.Unlock()
	return out
}

func (v *listeners) trigger() {
	for _, l := range v.listeners {
		l.trigger(v.bufferedEvents)
	}
	v.bufferedEvents = nil
}

func (l *Listener) Stop() {
	l.all.lock.Lock()
	defer l.all.lock.Unlock()

	l.cancel()
	l.wg.Wait()
	close(l.C)
	delete(l.all.listeners, l.id)
}

func (l *Listener) trigger(events Events) {
	if len(events) == 0 {
		return
	}

	l.wg.Add(1)
	go func() {
		defer l.wg.Done()
		select {
		case <-l.ctx.Done():
			// stop goroutine on stop/shutdown
		case l.C <- events:
			// propagate events, wait for other side
		}
	}()
}
