package atomic

import "sync"

type Counter struct {
	lock  sync.Mutex
	value int
}

func (c *Counter) Inc() {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.value++
}

func (c *Counter) Get() int {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.value
}

func (c *Counter) IncAndGet() int {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.value++
	return c.value
}

func (c *Counter) GetAndInc() int {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.value++
	return c.value - 1
}

func NewCounter(value int) *Counter {
	return &Counter{value: value}
}
