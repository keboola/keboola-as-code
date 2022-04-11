package diff

import (
	"fmt"

	"github.com/davecgh/go-spew/spew"
	"github.com/google/go-cmp/cmp"
)

// Reporter collects the differences during diff process.
type Reporter struct {
	differ      *differ
	a           Object      // A object
	b           Object      // B object
	path        cmp.Path    // current path to the compared value
	differences ResultItems // reported differences
}

func newReporter(d *differ, a, b Object) *Reporter {
	return &Reporter{differ: d, a: a, b: b}
}

func (r *Reporter) PushStep(ps cmp.PathStep) {
	r.path = append(r.path, ps)
}

func (r *Reporter) PopStep() {
	r.path = r.path[:len(r.path)-1]
}

func (r *Reporter) Differences() ResultItems {
	return r.differences
}

func (r *Reporter) Report(rs cmp.Result) {
	if rs.Equal() {
		// Only different values are processed.
		return
	}

	// Set A and B value
	result := &ResultItem{}
	result.Path = PathFromCmpPath(r.path)
	fmt.Println(r.path.GoString())
	fmt.Println(result.Path.String())
	s := spew.NewDefaultConfig()
	s.DisableMethods = true
	s.Dump(result.Path.Last())
	result.A, result.B = result.Path.Last().A(), result.Path.Last().B()
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
}
