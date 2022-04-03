package diff

import (
	"github.com/google/go-cmp/cmp"

	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/state/diff/transformer"
)

const MaxEqualLinesInString = 5 // maximum of equal lines returned by strings diff

// Reporter contains path to the compared values and generates human-readable difference report.
type Reporter struct {
	a           Object
	b           Object
	naming      *naming.Registry
	transformer *transformer.Transformer
	path        cmp.Path // current path to the compared value
	results     []*ResultValue
}

func newReporter(a, b Object, naming *naming.Registry) *Reporter {
	return &Reporter{a: a, b: b, naming: naming, transformer: transformer.NewTransformer(naming)}
}

// Options defines customization of the diff process.
func (r *Reporter) Options() cmp.Options {
	return append(cmp.Options{cmp.Reporter(r)}, r.transformer.Options())
}

func (r *Reporter) PushStep(ps cmp.PathStep) {
	r.path = append(r.path, ps)
}

func (r *Reporter) PopStep() {
	r.path = r.path[:len(r.path)-1]
}

func (r *Reporter) Results() []*ResultValue {
	return r.results
}

func (r *Reporter) Report(rs cmp.Result) {
	if rs.Equal() {
		// Only different values are processed.
		return
	}

	// Set A and B value
	result := &ResultValue{}
	result.A, result.B = r.path.Last().Values()
	r.results = append(r.results, result)

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
