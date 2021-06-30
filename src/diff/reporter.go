package diff

import (
	"fmt"
	"github.com/google/go-cmp/cmp"
	"strings"
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
		r.diffs = append(r.diffs, fmt.Sprintf("\t- %+v\n\t+ %+v", vx, vy))
	}
}

func (r *Reporter) PopStep() {
	r.path = r.path[:len(r.path)-1]
}

func (r *Reporter) String() string {
	return strings.Join(r.diffs, "\n")
}
