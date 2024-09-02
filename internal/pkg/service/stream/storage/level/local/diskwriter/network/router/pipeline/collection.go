package pipeline

import (
	"context"
	"sync"
	"time"

	"golang.org/x/exp/maps"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

// Collection for SinkPipeline and SlicePipeline types.
type Collection[K comparable, P pipeline[K]] struct {
	logger    log.Logger
	lock      sync.RWMutex
	pipelines map[K]P
	onUpdate  []func(context.Context, *Collection[K, P])
	onEmpty   []func(context.Context, *Collection[K, P])
}

type pipeline[K comparable] interface {
	Key() K
	Type() string
	Close(ctx context.Context, cause string) error
}

func NewCollection[K comparable, P pipeline[K]](logger log.Logger) *Collection[K, P] {
	return &Collection[K, P]{
		logger:    logger,
		pipelines: make(map[K]P),
	}
}

// OnUpdate adds on update callback.
func (c *Collection[K, P]) OnUpdate(fn func(context.Context, *Collection[K, P])) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.onUpdate = append(c.onUpdate, fn)
}

// OnEmpty adds on empty callback.
func (c *Collection[K, P]) OnEmpty(fn func(context.Context, *Collection[K, P])) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.onEmpty = append(c.onEmpty, fn)
}

func (c *Collection[K, P]) Get(k K) P {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.pipelines[k]
}

func (c *Collection[K, P]) Len() int {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return len(c.pipelines)
}

func (c *Collection[K, P]) All() (out []P) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	for _, p := range c.pipelines {
		out = append(out, p)
	}
	return out
}

// Register pipeline after open.
func (c *Collection[K, P]) Register(ctx context.Context, k K, p P) {
	defer c.callOnUpdate(ctx)

	c.lock.Lock()
	defer c.lock.Unlock()

	c.pipelines[k] = p
}

// Unregister pipeline before close.
func (c *Collection[K, P]) Unregister(ctx context.Context, k K) {
	var l int
	defer func() {
		if l == 0 {
			c.callOnEmpty(ctx)
		}
	}()

	defer c.callOnUpdate(ctx)

	c.lock.Lock()
	defer c.lock.Unlock()

	delete(c.pipelines, k)
	l = len(c.pipelines)
}

func (c *Collection[K, P]) Swap(ctx context.Context, replace []P) (old []P) {
	defer c.callOnUpdate(ctx)

	c.lock.Lock()
	defer c.lock.Unlock()

	old = maps.Values(c.pipelines)

	c.pipelines = make(map[K]P)
	for _, p := range replace {
		c.pipelines[p.Key()] = p
	}

	return old
}

// Close all pipelines in parallel.
func (c *Collection[K, P]) Close(ctx context.Context, cause string) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	wg := &sync.WaitGroup{}
	defer wg.Wait()

	for _, p := range c.All() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := p.Close(ctx, cause); err != nil {
				c.logger.Errorf(ctx, "cannot close %s pipeline: %s", p.Type(), err)
			}
		}()
	}
}

func (c *Collection[K, P]) callOnUpdate(ctx context.Context) {
	for _, fn := range c.onUpdate {
		fn(ctx, c)
	}
}

func (c *Collection[K, P]) callOnEmpty(ctx context.Context) {
	for _, fn := range c.onEmpty {
		fn(ctx, c)
	}
}
