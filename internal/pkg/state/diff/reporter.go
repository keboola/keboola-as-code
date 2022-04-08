package diff

import (
	"github.com/google/go-cmp/cmp"

	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local/naming"
)

// Reporter contains path to the compared values and generates human-readable difference report.
type Reporter struct {
	a           Object
	b           Object
	naming      *naming.Registry
	path        cmp.Path // current path to the compared value
	differences ResultValues
}

func newReporter(a, b Object, naming *naming.Registry) *Reporter {
	return &Reporter{a: a, b: b, naming: naming}
}

func (r *Reporter) PushStep(ps cmp.PathStep) {
	r.path = append(r.path, ps)
}

func (r *Reporter) PopStep() {
	r.path = r.path[:len(r.path)-1]
}

func (r *Reporter) Differences() ResultValues {
	return r.differences
}

func (r *Reporter) Report(rs cmp.Result) {
	if rs.Equal() {
		// Only different values are processed.
		return
	}

	// Set A and B value
	result := &ResultValue{}
	result.A, result.B = r.path.Last().Values()
	r.differences = append(r.differences, result)

	// Set state
	switch {
	case result.A.IsValid() && !result.B.IsValid():
		result.State = ResultOnlyInA
	case !result.A.IsValid() && result.B.IsValid():
		result.State = ResultOnlyInB
	default:
		result.State = ResultNotEqual
	}

	// Set path
	result.Path = PathFromCmpPath(r.path, r.naming)

	// Copy object path to the result if possible
	if s, ok := result.Path.Last().(StepObject); ok {
		result.FsPath = s.Path
	}
}
