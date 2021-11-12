package diff

import (
	"bytes"
	"fmt"
	"math"
	"reflect"
	"strings"

	"github.com/google/go-cmp/cmp"
	diffstr "github.com/kylelemons/godebug/diff"
	"github.com/spf13/cast"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

const MaxEqualLinesInString = 5 // maximum of equal lines returned by strings diff

// Reporter contains path to the compared values and generates human-readable difference report.
type Reporter struct {
	manifest     model.Record
	remoteObject model.Object
	localObject  model.Object
	state        *model.State // state of the other objects (to get objects path if needed)
	path         cmp.Path     // current path to the compared value
	paths        []string     // list of the non-equal paths
	diffs        []string     // list of the found differences in human-readable format
}

func newReporter(objectState model.ObjectState, state *model.State) *Reporter {
	return &Reporter{
		manifest:     objectState.Manifest(),
		remoteObject: objectState.RemoteState(),
		localObject:  objectState.LocalState(),
		state:        state,
	}
}

func (r *Reporter) PushStep(ps cmp.PathStep) {
	r.path = append(r.path, ps)
}

func (r *Reporter) Report(rs cmp.Result) {
	if !rs.Equal() {
		var lines []string
		remoteValue, localValue := r.path.Last().Values()

		// Diff values
		if v, ok := r.relationsDiff(remoteValue, localValue); ok {
			// Relations diff
			lines = append(lines, v...)
		} else if v, ok := r.stringsDiff(remoteValue, localValue); ok {
			// Strings diff
			lines = append(lines, v...)
		} else {
			// Other types
			lines = append(lines, valuesDiff(remoteValue, localValue)...)
		}

		// Format lines
		var mark string
		switch {
		case remoteValue.IsValid() && !localValue.IsValid():
			mark = OnlyInRemoteMark
		case !remoteValue.IsValid() && localValue.IsValid():
			mark = OnlyInLocalMark
		default:
			// Values are present in both: local and remote value
			// Individual lines are already marked.
			mark = " "
		}
		pathStr := r.pathToString(r.path)
		if len(pathStr) > 0 {
			r.paths = append(r.paths, pathStr)
			if !r.isPathHidden() {
				r.diffs = append(r.diffs, fmt.Sprintf(`%s %s:`, mark, pathStr))
				// Ident values inside path
				mark += `  `
			}
		}
		for _, line := range lines {
			r.diffs = append(r.diffs, fmt.Sprintf(`%s %s`, mark, line))
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

func (r *Reporter) relationsDiff(remoteValue, localValue reflect.Value) ([]string, bool) {
	relationsType := reflect.TypeOf((*model.Relations)(nil)).Elem()
	if remoteValue.IsValid() && localValue.IsValid() && remoteValue.Type().ConvertibleTo(relationsType) && localValue.Type().ConvertibleTo(relationsType) {
		onlyInRemote, onlyInLocal := remoteValue.Interface().(model.Relations).Diff(localValue.Interface().(model.Relations))
		var out []string
		for _, v := range onlyInRemote {
			out = append(out, fmt.Sprintf("%s %s", OnlyInRemoteMark, r.relationToString(v, r.remoteObject, r.state.RemoteObjects())))
		}
		for _, v := range onlyInLocal {
			out = append(out, fmt.Sprintf("%s %s", OnlyInLocalMark, r.relationToString(v, r.localObject, r.state.LocalObjects())))
		}
		return out, true
	}
	return nil, false
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

func (r *Reporter) stringsDiff(remoteValue, localValue reflect.Value) ([]string, bool) {
	if remoteValue.IsValid() && localValue.IsValid() && remoteValue.Type().String() == `string` && localValue.Type().String() == `string` {
		diff := stringsDiff(remoteValue.Interface().(string), localValue.Interface().(string))
		return strings.Split(diff, "\n"), true
	}
	return nil, false
}

func (r *Reporter) pathToString(path cmp.Path) string {
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

func (r *Reporter) objectPath(value reflect.Value) string {
	if value.IsValid() {
		if v, ok := value.Interface().(model.RecordPaths); ok {
			objectPath := v.Path()
			if objectPath != "" {
				if v, err := filesystem.Rel(r.manifest.Path(), objectPath); err == nil {
					return v
				}
			}
		}
	}
	return ""
}

func valuesDiff(remote, local reflect.Value) []string {
	var out []string

	// Resolve interfaces
	var remoteType reflect.Type
	var localType reflect.Type
	if remote.IsValid() {
		remoteType = remote.Type()
		if remoteType.Kind().String() == `interface` {
			remote = remote.Elem()
			remoteType = remote.Type()
		}
	}
	if local.IsValid() {
		localType = local.Type()
		if localType.Kind().String() == `interface` {
			local = local.Elem()
			localType = local.Type()
		}
	}

	// Print types if differs
	includeType := remote.IsValid() && local.IsValid() && remoteType.String() != localType.String()

	// Format values
	if remote.IsValid() {
		formatted := formatValue(remote, remoteType, includeType)
		if len(formatted) != 0 {
			valueMark := ``
			if local.IsValid() {
				valueMark = OnlyInRemoteMark + ` `
			}
			for _, line := range formatted {
				out = append(out, fmt.Sprintf("%s%s", valueMark, line))
			}
		}
	}
	if local.IsValid() {
		formatted := formatValue(local, localType, includeType)
		if len(formatted) != 0 {
			valueMark := ``
			if remote.IsValid() {
				valueMark = OnlyInLocalMark + ` `
			}
			for _, line := range formatted {
				out = append(out, fmt.Sprintf("%s%s", valueMark, line))
			}
		}
	}
	return out
}

func formatValue(value reflect.Value, t reflect.Type, includeType bool) []string {
	var formatted string
	switch {
	case strings.HasPrefix(t.String(), "map["):
		// Format map to JSON
		formatted = strings.TrimRight(json.MustEncodeString(value.Interface(), true), "\n")
	case includeType:
		formatted = fmt.Sprintf(`%#v`, value)
	default:
		formatted = fmt.Sprintf(`%+v`, value)
	}
	return strings.Split(formatted, "\n")
}

func stringsDiff(remote, local string) string {
	remoteLines := strings.Split(remote, "\n")
	if len(remote) == 0 {
		remoteLines = []string{}
	}
	localLines := strings.Split(local, "\n")
	if len(local) == 0 {
		localLines = []string{}
	}
	chunks := diffstr.DiffChunks(remoteLines, localLines)
	out := new(bytes.Buffer)
	for _, c := range chunks {
		for _, line := range c.Added {
			_, _ = fmt.Fprintf(out, "%s %s\n", OnlyInLocalMark, line)
		}
		for _, line := range c.Deleted {
			_, _ = fmt.Fprintf(out, "%s %s\n", OnlyInRemoteMark, line)
		}
		for i, line := range c.Equal {
			// Limit number of equal lines in row
			if i+1 >= MaxEqualLinesInString && len(c.Equal) > MaxEqualLinesInString {
				_, _ = fmt.Fprint(out, "  ...\n")
				break
			}
			if len(line) == 0 {
				_, _ = fmt.Fprint(out, "\n")
			} else {
				_, _ = fmt.Fprintf(out, "  %s\n", line)
			}
		}
	}
	return strings.TrimRight(out.String(), "\n")
}
