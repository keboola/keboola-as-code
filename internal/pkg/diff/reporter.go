package diff

import (
	"fmt"
	"strings"

	"github.com/google/go-cmp/cmp"
	"github.com/spf13/cast"
)

type Reporter struct {
	path  cmp.Path
	diffs []string
}

func (r *Reporter) PushStep(ps cmp.PathStep) {
	r.path = append(r.path, ps)
}

func (r *Reporter) Report(rs cmp.Result) {
	if !rs.Equal() {
		vx, vy := r.path.Last().Values()
		pathStr := pathToString(r.path)
		if len(pathStr) > 0 {
			r.diffs = append(r.diffs, fmt.Sprintf("  \"%s\":", pathStr))
		}
		if vx.IsValid() {
			r.diffs = append(r.diffs, fmt.Sprintf("  %s %+v", OnlyInRemoteMark, vx))
		}
		if vy.IsValid() {
			r.diffs = append(r.diffs, fmt.Sprintf("  %s %+v", OnlyInLocalMark, vy))
		}
	}
}

func (r *Reporter) PopStep() {
	r.path = r.path[:len(r.path)-1]
}

func (r *Reporter) String() string {
	return strings.Join(r.diffs, "\n")
}

func pathToString(path cmp.Path) string {
	var parts []string
	skip := make(map[int]bool)

	for i, s := range path {
		if skip[i] {
			continue
		}

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
			parts = append(parts, cast.ToString(v.Key()))
		case cmp.StructField:
			parts = append(parts, v.Name())
		}
	}
	return strings.Join(parts, ".")
}
