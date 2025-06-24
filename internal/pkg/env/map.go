package env

import (
	"fmt"
	"maps"
	"os"
	"sort"
	"strings"

	"github.com/joho/godotenv"
	"github.com/sasha-s/go-deadlock"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// Provider is read-only interface to get ENV value.
type Provider interface {
	Lookup(key string) (string, bool)
	Get(key string) string
	MustGet(key string) string
	ToSlice() []string
}

// Map - abstraction for ENV variables.
// Keys are represented as uppercase string.
type Map struct {
	data map[string]string
	lock *deadlock.RWMutex
}

func Empty() *Map {
	return &Map{
		data: make(map[string]string),
		lock: &deadlock.RWMutex{},
	}
}

func FromMap(data map[string]string) *Map {
	m := Empty()
	for k, v := range data {
		m.Set(k, v)
	}
	return m
}

func FromOs() (*Map, error) {
	m := Empty()
	for _, pair := range os.Environ() {
		parts := strings.SplitN(pair, "=", 2)
		if len(parts) == 2 {
			m.Set(parts[0], parts[1])
		}
	}

	return m, nil
}

func (m *Map) Clone() *Map {
	out := Empty()
	for k, v := range m.data {
		out.Set(k, v)
	}
	return out
}

func (m *Map) ToString() (string, error) {
	return godotenv.Marshal(m.data)
}

func (m *Map) ToSlice() []string {
	out := make([]string, 0, len(m.Keys()))
	for _, k := range m.Keys() {
		v := m.Get(k)
		out = append(out, fmt.Sprintf(`%s=%s`, k, v))
	}
	sort.Strings(out)
	return out
}

func (m *Map) ToMap() map[string]string {
	data := make(map[string]string)
	maps.Copy(data, m.data)
	return data
}

func (m *Map) Keys() []string {
	keys := make([]string, 0, len(m.data))
	for k := range m.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
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

func (m *Map) GetOrErr(key string) (string, error) {
	value := m.Get(key)
	if len(value) == 0 {
		return "", errors.Errorf("missing ENV variable \"%s\"", strings.ToUpper(key))
	}
	return value, nil
}

func (m *Map) MustGet(key string) string {
	value := m.Get(key)
	if len(value) == 0 {
		panic(errors.Errorf("missing ENV variable \"%s\"", strings.ToUpper(key)))
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
