package env

import (
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/joho/godotenv"
)

// Map - abstraction for ENV variables.
// Keys are represented as uppercase string.
type Map struct {
	data map[string]string
	lock *sync.RWMutex
}

func Empty() *Map {
	return &Map{
		data: make(map[string]string),
		lock: &sync.RWMutex{},
	}
}

func FromOs() (*Map, error) {
	m := Empty()
	envs, err := godotenv.Unmarshal(strings.Join(os.Environ(), "\n"))
	if err != nil {
		return nil, err
	}

	for k, v := range envs {
		m.Set(k, v)
	}

	return m, nil
}

func (m *Map) ToString() (string, error) {
	return godotenv.Marshal(m.data)
}

func (m *Map) Keys() []string {
	keys := make([]string, 0)
	for k := range m.data {
		keys = append(keys, k)
	}
	return keys
}

func (m *Map) Clear() {
	m.data = make(map[string]string)
}

func (m *Map) Lookup(key string) (string, bool) {
	value, found := m.data[strings.ToUpper(key)]
	return value, found
}

func (m *Map) Get(key string) string {
	m.lock.RLock()
	defer m.lock.RUnlock()
	return m.data[strings.ToUpper(key)]
}

func (m *Map) MustGet(key string) string {
	value := m.Get(key)
	if len(value) == 0 {
		panic(fmt.Errorf("missing ENV variable \"%s\"", strings.ToUpper(key)))
	}
	return value
}

func (m *Map) Set(key, value string) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.data[strings.ToUpper(key)] = value
}

func (m *Map) Unset(key string) {
	delete(m.data, strings.ToUpper(key))
}

// Merge keys from an env.Map.
func (m *Map) Merge(data *Map, overwrite bool) {
	for k, v := range data.data {
		if _, found := m.Lookup(k); found && !overwrite {
			continue
		}
		m.Set(k, v)
	}
}
