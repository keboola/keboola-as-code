package diff

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/google/go-cmp/cmp"
	"github.com/spf13/cast"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

// Reporter contains path to the compared values and generates human-readable difference report.
type Reporter struct {
	objectKey model.Key    // key of the root object, parent of the compared values
	state     *model.State // state of the other objects (to get objects path if needed)
	path      cmp.Path     // current path to the compared value
	paths     []string     // list of the non-equal paths
	diffs     []string     // list of the found differences in human-readable format
}

func newReporter(objectKey model.Key, state *model.State) *Reporter {
	return &Reporter{
		objectKey: objectKey,
		state:     state,
	}
}

func (r *Reporter) PushStep(ps cmp.PathStep) {
	r.path = append(r.path, ps)
}

func (r *Reporter) Report(rs cmp.Result) {
	if !rs.Equal() {
		vx, vy := r.path.Last().Values()
		pathStr := pathToString(r.path)
		if len(pathStr) > 0 {
			r.paths = append(r.paths, pathStr)
			r.diffs = append(r.diffs, fmt.Sprintf("  \"%s\":", pathStr))
		}

		// Format relations diff
		if r.relationsDiff(vx, vy) {
			return
		}

		// Other types
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

func (r *Reporter) Paths() []string {
	return r.paths
}

func (r *Reporter) relationsDiff(vx, vy reflect.Value) bool {
	relationsType := reflect.TypeOf((*model.Relations)(nil)).Elem()
	if vx.IsValid() && vy.IsValid() && vx.Type().ConvertibleTo(relationsType) && vy.Type().ConvertibleTo(relationsType) {
		onlyInRemote, onlyInLocal := vx.Interface().(model.Relations).Diff(vy.Interface().(model.Relations))
		for _, v := range onlyInRemote {
			r.diffs = append(r.diffs, fmt.Sprintf("  %s %s", OnlyInRemoteMark, r.relationToString(v)))
		}
		for _, v := range onlyInLocal {
			r.diffs = append(r.diffs, fmt.Sprintf("  %s %s", OnlyInLocalMark, r.relationToString(v)))
		}
		return true
	}
	return false
}

func (r *Reporter) relationToString(relation model.Relation) string {
	otherSideDesc := ``
	otherSideKey := relation.OtherSideKey(r.objectKey)
	if otherSide, found := r.state.Get(otherSideKey); found {
		otherSideDesc = `"` + otherSide.Path() + `"`
	}
	if len(otherSideDesc) == 0 {
		otherSideDesc = otherSideKey.Desc()
	}
	return relation.Desc() + ` ` + otherSideDesc
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
