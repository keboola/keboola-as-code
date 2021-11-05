package model

import (
	"sort"
	"strings"
)

type ChangedFields map[string]bool

func (f ChangedFields) IsEmpty() bool {
	for _, changed := range f {
		if changed {
			return false
		}
	}
	return true
}

func (f ChangedFields) String() string {
	var out []string
	for field, changed := range f {
		if changed {
			out = append(out, field)
		}
	}
	sort.Strings(out)
	return strings.Join(out, `, `)
}
