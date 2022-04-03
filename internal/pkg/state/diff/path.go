package diff

import (
	"math"
	"strings"

	"github.com/google/go-cmp/cmp"
	"github.com/spf13/cast"
)

type Path []PathStep

type PathStep struct {
}

func PathFromCmpPath(path cmp.Path) Path {
	var parts []string
	skip := make(map[int]bool)

	for i, s := range path {
		if skip[i] {
			continue
		}

		// Use object path if present
		remote, local := s.Values()
		if v := r.objectPath(local); v != "" {
			parts = []string{v}
			continue
		}
		if v := r.objectPath(remote); v != "" {
			parts = []string{v}
			continue
		}

		// Append path by type
		switch v := s.(type) {
		case cmp.Transform:
			// strByLine is transform to compare strings line by line
			// ... so we skip next part - []string index
			if v.Name() == `strByLine` {
				skip[i+1] = true
			}
		case cmp.MapIndex:
			parts = append(parts, cast.ToString(v.Key().Interface()))
		case cmp.SliceIndex:
			// index1 or index2 can be "-1",
			// if the value is on one side only
			index1, index2 := v.SplitKeys()
			parts = append(parts, cast.ToString(math.Max(float64(index1), float64(index2))))
		case cmp.StructField:
			parts = append(parts, v.Name())
		}
	}

	return strings.Join(parts, ".")
}
