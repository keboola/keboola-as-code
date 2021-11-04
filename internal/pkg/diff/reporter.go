package diff

import (
	"fmt"
	"strings"

	"github.com/google/go-cmp/cmp"
	"github.com/spf13/cast"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

// Reporter contains path to the compared values and generates human-readable difference report.
type Reporter struct {
	path     cmp.Path
	valueX   interface{}
	valueY   interface{}
	parentsX []interface{}
	parentsY []interface{}
	diffs    []string
}

func newReporter(xRoot, yRoot interface{}) Reporter {
	r := Reporter{}
	r.valueX = xRoot
	r.valueY = yRoot
	return r
}

func (r *Reporter) PushStep(ps cmp.PathStep) {
	r.path = append(r.path, ps)
	r.parentsX = append(r.parentsX, r.valueX)
	r.parentsY = append(r.parentsY, r.valueY)
	r.valueX, r.valueY = ps.Values()
}

func (r *Reporter) ParentKey() model.Key {
	if o, ok := r.ParentX().(model.Object); ok {
		return o.Key()
	} else if o, ok := r.ParentY().(model.Object); ok {
		return o.Key()
	}
	return nil
}

func (r *Reporter) ParentX() interface{} {
	s := r.parentsX
	if i := len(s) - 1; i != -1 {
		return s[i]
	}
	return nil
}

func (r *Reporter) ParentY() interface{} {
	s := r.parentsY
	if i := len(s) - 1; i != -1 {
		return s[i]
	}
	return nil
}

func (r *Reporter) Report(rs cmp.Result) {
	if !rs.Equal() {
		vx, vy := r.path.Last().Values()
		pathStr := pathToString(r.path)
		if len(pathStr) > 0 {
			r.diffs = append(r.diffs, fmt.Sprintf("  \"%s\":", pathStr))
		}
		if vx.IsValid() {
			formatted := fmt.Sprintf(`%+v`, vx)
			if len(formatted) != 0 {
				r.diffs = append(r.diffs, fmt.Sprintf("  %s %+v", OnlyInRemoteMark, formatted))
			}
		}
		if vy.IsValid() {
			formatted := fmt.Sprintf(`%+v`, vy)
			if len(formatted) != 0 {
				r.diffs = append(r.diffs, fmt.Sprintf("  %s %+v", OnlyInLocalMark, formatted))
			}
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
