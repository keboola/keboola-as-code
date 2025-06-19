package configpatch

import (
	"sort"
	"strings"
)

// ConfigKV is result of the patch operation for a one configuration key.
type ConfigKV struct {
	// KeyPath is a configuration key, parts are joined with a dot "." separator.
	KeyPath string `json:"key"`
	// Type is primitive type of the serialized value.
	Type string `json:"type"`
	// Description of the key.
	Description string `json:"description"`
	// Value is an actual value of the configuration key.
	Value any `json:"value"`
	// DefaultValue of the configuration key.
	DefaultValue any `json:"defaultValue"`
	// Overwritten is true, if the DefaultValue was replaced by a value from the path.
	Overwritten bool `json:"overwritten"`
	// Protected configuration key can be modified only by a super-admin user.
	Protected bool `json:"protected"`
	// Validation contains validation rules of the field, if any.
	Validation string `json:"validation,omitempty"`
}

// PatchKV is a one configuration key from a configuration patch.
type PatchKV struct {
	// KeyPath is a configuration key, parts are joined with a dot "." separator.
	KeyPath string `json:"key"`
	// Value is a patched value of the configuration key.
	Value any `json:"value"`
}

type PatchKVs []PatchKV

func (v PatchKVs) With(slices ...PatchKVs) (out PatchKVs) {
	// Merge and deduplicate, the later value has priority
	keys := make(map[string]PatchKV)
	for _, item := range v {
		keys[item.KeyPath] = item
	}
	for _, slice := range slices {
		for _, item := range slice {
			keys[item.KeyPath] = item
		}
	}

	// Map -> slice
	for _, item := range keys {
		out = append(out, item)
	}

	// Sort
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].KeyPath < out[j].KeyPath
	})

	return out
}

// In descents and returns only keys in the prefix.
func (v PatchKVs) In(prefix string) (out PatchKVs) {
	prefix = strings.TrimSuffix(prefix, ".") + "."
	for _, item := range v {
		if after, ok := strings.CutPrefix(item.KeyPath, prefix); ok {
			item.KeyPath = after
			out = append(out, item)
		}
	}

	// Sort
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].KeyPath < out[j].KeyPath
	})

	return out
}
