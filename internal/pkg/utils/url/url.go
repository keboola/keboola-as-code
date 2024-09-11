package url

import (
	"net/url"
	"strconv"
	"strings"

	"github.com/keboola/go-utils/pkg/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// ParseQuery is taken from net/url package but returns ordered map instead of regular map.
// It is also adjusted to work better with nested keys.
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
		key, err = url.QueryUnescape(key)
		if err != nil {
			return m, err
		}
		value, err = url.QueryUnescape(value)
		if err != nil {
			return m, err
		}

		path, sliceAppend, err := parseKey(key)
		if err != nil {
			return m, err
		}

		if sliceAppend {
			existingValue, found, _ := m.GetNestedPath(path)
			if !found {
				err = m.SetNestedPath(path, []any{value})
				if err != nil {
					return m, err
				}
			} else {
				if existingValueSlice, ok := existingValue.([]any); ok {
					err = m.SetNestedPath(path, append(existingValueSlice, value))
					if err != nil {
						return m, err
					}
				} else {
					err = errors.Errorf("invalid square brackets in query")
					return m, err
				}
			}
			continue
		}

		err = m.SetNestedPath(path, value)
		if err != nil {
			return m, err
		}
	}
	return m, err
}

// parseKey transforms a given url key string to orderedmap.Path.
// "key[subkey]" would give the same result as orderedmap.PathFromStr("key.subkey")
// "key[123]" would give the same result as orderedmap.PathFromStr("key[123]")
// [] at the end of the string will cause sliceAppend = true.
func parseKey(str string) (path orderedmap.Path, sliceAppend bool, err error) {
	parts := strings.FieldsFunc(str, func(r rune) bool {
		return r == '['
	})

	for i, part := range parts {
		if i == 0 {
			path = append(path, orderedmap.MapStep(part))
			continue
		}
		if part[len(part)-1] != ']' {
			err = errors.Errorf(`Unable to parse key "%s"`, str)
			return path, sliceAppend, err
		}
		part = part[:len(part)-1]
		if part == "" {
			if i == len(parts)-1 {
				sliceAppend = true
			} else {
				err = errors.Errorf(`Unable to parse key "%s"`, str)
			}
			return path, sliceAppend, err
		}

		if v, err := strconv.ParseInt(part, 10, 64); err == nil {
			path = append(path, orderedmap.SliceStep(v))
		} else {
			path = append(path, orderedmap.MapStep(part))
		}
	}

	return path, sliceAppend, err
}
