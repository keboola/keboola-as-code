package distribution

import (
	"context"
	"sync"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// Listener listens for distribution changes, when a node is added or removed.
// It contains the C channel with distribution change Events.
type Listener struct {
	C      <-chan Events
	c      chan Events
	ctx    context.Context
	cancel context.CancelCauseFunc
	wg     *sync.WaitGroup
	all    *listeners
	id     listenerID
}

type listeners struct {
	config         Config
	lock           *sync.Mutex
	bufferedEvents Events
	listeners      map[listenerID]*Listener
}

type listenerID string

func newListeners(ctx context.Context, wg *sync.WaitGroup, cfg Config, logger log.Logger, d dependencies) *listeners {
	logger = logger.WithComponent("listeners")

	v := &listeners{
		config:    cfg,
		lock:      &sync.Mutex{},
		listeners: make(map[listenerID]*Listener),
	}

	wg.Go(func() {

		// If the interval > 0, then listeners are not triggered immediately on change,
		// but all events within the groupInterval are processed at once.
		// Otherwise, trigger is called immediately, see Notify method.
		var tickerC <-chan time.Time
		if v.config.EventsGroupInterval > 0 {
			ticker := d.Clock().NewTicker(v.config.EventsGroupInterval)
			defer ticker.Stop()
			tickerC = ticker.Chan()
		}

		for {
			select {
			case <-ctx.Done():
				// Handle shutdown
				v.lock.Lock()

				// Log info
				count := len(v.listeners)
				if count > 0 {
					logger.Infof(ctx, `waiting for "%d" listeners`, count)
				}

				// Process remaining events
				v.trigger()

				// Stop all listeners
				for _, l := range v.listeners {
					l.cancel(errors.New("listener: context done"))
					l.wg.Wait()
				}

				v.listeners = nil
				v.lock.Unlock()
				return
			case <-tickerC:
				// Trigger listeners at most once per "group interval"
				v.lock.Lock()
				v.trigger()
				v.lock.Unlock()
			}
		}
	})

	return v
}

func (v *listeners) Reset() {
	v.lock.Lock()
	v.bufferedEvents = nil
	v.lock.Unlock()
}

// Notify listeners about a new event. The event is not processed immediately.
// All events within the "group interval" are processed at once, see trigger method.
func (v *listeners) Notify(events Events) {
	v.lock.Lock()
	defer v.lock.Unlock()

	// All events within the "group interval" are processed at once.
	v.bufferedEvents = append(v.bufferedEvents, events...)

	// Trigger listeners immediately, if there is no grouping interval
	if v.config.EventsGroupInterval == 0 {
		v.trigger()
	}
}

// add a new listener, it contains channel C with streamed distribution change Events.
func (v *listeners) add() *Listener {
	c := make(chan Events)

	ctx, cancel := context.WithCancelCause(context.Background())
	out := &Listener{
		ctx:    ctx,
		cancel: cancel,
		wg:     &sync.WaitGroup{},
		all:    v,
		id:     listenerID(idgenerator.Random(10)),
		C:      c,
		c:      c,
	}
	v.lock.Lock()
	v.listeners[out.id] = out
	v.lock.Unlock()
	return out
}

// trigger sends all buffered events to all subscribed listeners and clears the buffer.
// Should only be called while lock is held.
func (v *listeners) trigger() {
	for _, l := range v.listeners {
		l.trigger(v.bufferedEvents)
	}
	v.bufferedEvents = nil
}

func (l *Listener) Wait(ctx context.Context) (Events, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case events := <-l.C:
		return events, nil
	}
}

func (l *Listener) Stop() {
	l.all.lock.Lock()
	defer l.all.lock.Unlock()

	l.cancel(errors.New("listener: stop"))
	l.wg.Wait()
	delete(l.all.listeners, l.id)
}

func (l *Listener) trigger(events Events) {
	if len(events) == 0 {
		return
	}

	l.wg.Go(func() {
		select {
		case <-l.ctx.Done():
			// stop goroutine on stop/shutdown
		case l.c <- events:
			// propagate events, wait for receiver side
		}
	})
}
