package model

import (
	"sort"
	"strings"
)

// ChangedFields - changed fields in diff.
type ChangedFields map[string]*ChangedField

// ChangedField one changed field, contains diff string and changed paths in any.
type ChangedField struct {
	paths map[string]bool
	diff  string
}

func NewChangedFields(fields ...string) ChangedFields {
	v := make(ChangedFields)
	for _, field := range fields {
		v[field] = newChangedField()
	}
	return v
}

func newChangedField() *ChangedField {
	return &ChangedField{paths: make(map[string]bool)}
}

func (v ChangedFields) IsEmpty() bool {
	return len(v) == 0
}

func (v ChangedFields) Add(field string) *ChangedField {
	if !v.Has(field) {
		v[field] = newChangedField()
	}
	return v[field]
}

func (v ChangedFields) Get(field string) *ChangedField {
	v.Add(field)
	return v[field]
}

func (v ChangedFields) Has(field string) bool {
	_, found := v[field]
	return found
}

func (v ChangedFields) Remove(field string) {
	delete(v, field)
}

func (v ChangedFields) String() string {
	var out []string
	for field := range v {
		out = append(out, field)
	}
	sort.Strings(out)
	return strings.Join(out, `, `)
}

func (v *ChangedField) SetDiff(diff string) *ChangedField {
	v.diff = diff
	return v
}

func (v *ChangedField) Diff() string {
	return v.diff
}

func (v *ChangedField) AddPath(paths ...string) *ChangedField {
	for _, path := range paths {
		if !v.HasPath(path) {
			v.paths[path] = true
		}
	}
	return v
}

func (v *ChangedField) HasPath(path string) bool {
	return v.paths[path]
}

func (v *ChangedField) RemovePath(path string) {
	delete(v.paths, path)
}

func (v *ChangedField) Paths() string {
	var out []string
	for path, changed := range v.paths {
		if changed {
			out = append(out, path)
		}
	}
	sort.Strings(out)
	return strings.Join(out, `, `)
}
