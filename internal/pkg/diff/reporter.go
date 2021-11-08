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
	remoteObject model.Object
	localObject  model.Object
	state        *model.State // state of the other objects (to get objects path if needed)
	path         cmp.Path     // current path to the compared value
	paths        []string     // list of the non-equal paths
	diffs        []string     // list of the found differences in human-readable format
}

func newReporter(remoteObject, localObject model.Object, state *model.State) *Reporter {
	return &Reporter{
		remoteObject: remoteObject,
		localObject:  localObject,
		state:        state,
	}
}

func (r *Reporter) PushStep(ps cmp.PathStep) {
	r.path = append(r.path, ps)
}

func (r *Reporter) Report(rs cmp.Result) {
	if !rs.Equal() {
		remoteValue, localValue := r.path.Last().Values()
		pathStr := pathToString(r.path)
		if len(pathStr) > 0 {
			r.paths = append(r.paths, pathStr)
			if !r.isPathHidden() {
				r.diffs = append(r.diffs, fmt.Sprintf("  \"%s\":", pathStr))
			}
		}

		// Format relations diff
		if r.relationsDiff(remoteValue, localValue) {
			return
		}

		// Other types
		if remoteValue.IsValid() {
			formatted := fmt.Sprintf(`%+v`, remoteValue)
			if len(formatted) != 0 {
				r.diffs = append(r.diffs, fmt.Sprintf("  %s %+v", OnlyInRemoteMark, formatted))
			}
		}
		if localValue.IsValid() {
			formatted := fmt.Sprintf(`%+v`, localValue)
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

func (r *Reporter) relationsDiff(remoteValue, localValue reflect.Value) bool {
	relationsType := reflect.TypeOf((*model.Relations)(nil)).Elem()
	if remoteValue.IsValid() && localValue.IsValid() && remoteValue.Type().ConvertibleTo(relationsType) && localValue.Type().ConvertibleTo(relationsType) {
		onlyInRemote, onlyInLocal := remoteValue.Interface().(model.Relations).Diff(localValue.Interface().(model.Relations))
		for _, v := range onlyInRemote {
			r.diffs = append(r.diffs, fmt.Sprintf("  %s %s", OnlyInRemoteMark, r.relationToString(v, r.remoteObject, r.state.RemoteObjects())))
		}
		for _, v := range onlyInLocal {
			r.diffs = append(r.diffs, fmt.Sprintf("  %s %s", OnlyInLocalMark, r.relationToString(v, r.localObject, r.state.LocalObjects())))
		}
		return true
	}
	return false
}

func (r *Reporter) isPathHidden() bool {
	// Hide InManifest/InKey paths from model.RelationsBySide
	return r.path.Last().Type().String() == "model.Relations"
}

func (r *Reporter) relationToString(relation model.Relation, definedOn model.Object, allObjects *model.StateObjects) string {
	otherSideDesc := ``
	otherSideKey, _, err := relation.NewOtherSideRelation(definedOn, allObjects)
	if err == nil {
		if otherSide, found := r.state.Get(otherSideKey); found {
			otherSideDesc = `"` + otherSide.Path() + `"`
		}
		if len(otherSideDesc) == 0 {
			otherSideDesc = otherSideKey.Desc()
		}
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
