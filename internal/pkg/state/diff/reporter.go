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

	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local/naming"
)

const MaxEqualLinesInString = 5 // maximum of equal lines returned by strings diff

// Reporter contains path to the compared values and generates human-readable difference report.
type Reporter struct {
	a      Object
	b      Object
	naming *naming.Registry
	path   cmp.Path // current path to the compared value
	paths  []string // list of the non-equal paths
	diffs  []string // list of the found differences in human-readable format
}

func newReporter(a, b Object, naming *naming.Registry) *Reporter {
	return &Reporter{a: a, b: b, naming: naming}
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
			lines = append(lines, r.valuesDiff(remoteValue, localValue)...)
		}

		// Format lines
		var mark string
		switch {
		case remoteValue.IsValid() && !localValue.IsValid():
			mark = OnlyInAMark
		case !remoteValue.IsValid() && localValue.IsValid():
			mark = OnlyInBMark
		default:
			// Values are present in both: local and remote value
			// Individual lines are already marked.
			mark = " "
		}
		if pathStr := r.pathToString(r.path); len(pathStr) > 0 {
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
			out = append(out, fmt.Sprintf("%s %s", OnlyInAMark, r.relationToString(v, r.a)))
		}
		for _, v := range onlyInLocal {
			out = append(out, fmt.Sprintf("%s %s", OnlyInBMark, r.relationToString(v, r.b)))
		}
		return out, true
	}
	return nil, false
}

func (r *Reporter) isPathHidden() bool {
	// Hide InManifest/InKey paths from model.RelationsBySide
	return r.path.Last().Type().String() == "model.Relations"
}

func (r *Reporter) relationToString(relation model.Relation, definedOn Object) string {
	otherSideDesc := ``
	otherSideKey, _, err := relation.NewOtherSideRelation(definedOn.Object, definedOn.All)
	if err == nil && otherSideKey != nil {
		if path, found := r.naming.PathByKey(otherSideKey); found {
			otherSideDesc = `"` + path.String() + `"`
		} else {
			otherSideDesc = otherSideKey.String()
		}
	}
	return relation.String() + ` ` + otherSideDesc
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
		if v, ok := value.Interface().(model.WithKey); ok {
			if path, found := r.naming.PathByKey(v.Key()); found {
				return path.RelativePath()
			}
		}
	}
	return ""
}

func (r *Reporter) valuesDiff(a, b reflect.Value) []string {
	var out []string

	// Resolve interfaces
	a, aType := coreType(a)
	b, bType := coreType(b)

	// Print types if differs
	includeType := a.IsValid() && b.IsValid() && !a.IsZero() && !b.IsZero() && aType.String() != bType.String()

	// Format values
	if a.IsValid() {
		formatted := r.formatValue(a, aType, includeType)
		if len(formatted) != 0 {
			valueMark := ``
			if b.IsValid() {
				valueMark = OnlyInAMark + ` `
			}
			for _, line := range formatted {
				out = append(out, fmt.Sprintf("%s%s", valueMark, line))
			}
		}
	}
	if b.IsValid() {
		formatted := r.formatValue(b, bType, includeType)
		if len(formatted) != 0 {
			valueMark := ``
			if a.IsValid() {
				valueMark = OnlyInBMark + ` `
			}
			for _, line := range formatted {
				out = append(out, fmt.Sprintf("%s%s", valueMark, line))
			}
		}
	}
	return out
}

func (r *Reporter) formatValue(value reflect.Value, t reflect.Type, includeType bool) []string {
	var formatted string
	phase, isPhase := value.Interface().(model.Phase)
	task, isTask := value.Interface().(model.Task)

	switch {
	case t.Kind() == reflect.Ptr && value.IsNil():
		formatted = `(null)`
	case t.Kind() == reflect.Map:
		// Format map to JSON
		formatted = strings.TrimRight(json.MustEncodeString(value.Interface(), true), "\n")
	case isPhase:
		return strings.Split(phaseToString(phase, r.naming), "\n")
	case isTask:
		return strings.Split(taskToString(task, r.naming), "\n")
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
			_, _ = fmt.Fprintf(out, "%s %s\n", OnlyInBMark, line)
		}
		for _, line := range c.Deleted {
			_, _ = fmt.Fprintf(out, "%s %s\n", OnlyInAMark, line)
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
