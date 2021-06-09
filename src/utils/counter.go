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

func (c *SafeCounter) Value() int {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.value
}
