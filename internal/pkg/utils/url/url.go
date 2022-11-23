package url

import (
	"net/url"
	"strings"

	"github.com/keboola/go-utils/pkg/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// ParseQuery is taken from net/url package but returns ordered map instead of regular map.
func ParseQuery(query string) (m *orderedmap.OrderedMap, err error) {
	m = orderedmap.New()
	for query != "" {
		var key string
		key, query, _ = strings.Cut(query, "&")
		if strings.Contains(key, ";") {
			err = errors.Errorf("invalid semicolon separator in query")
			continue
		}
		if key == "" {
			continue
		}
		key, value, _ := strings.Cut(key, "=")
		key, err1 := url.QueryUnescape(key)
		if err1 != nil {
			if err == nil {
				err = err1
			}
			continue
		}
		value, err1 = url.QueryUnescape(value)
		if err1 != nil {
			if err == nil {
				err = err1
			}
			continue
		}
		existingValue, found := m.Get(key)
		if found {
			if existingValueSlice, ok := existingValue.([]any); ok {
				m.Set(key, append(existingValueSlice, value))
			} else {
				m.Set(key, []any{existingValue, value})
			}
		} else {
			m.Set(key, value)
		}
	}
	return m, err
}
