package utils

import "sync"

type SafeCounter struct {
	lock  sync.Mutex
	value int
}

func (c *SafeCounter) Inc() {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.value++
}

func (c *SafeCounter) Get() int {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.value
}

func (c *SafeCounter) IncAndGet() int {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.value++
	return c.value
}

func NewSafeCounter(value int) *SafeCounter {
	return &SafeCounter{value: value}
}
