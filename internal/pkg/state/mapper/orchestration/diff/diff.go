package diff

import (
	"fmt"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state/diff"
	"github.com/keboola/keboola-as-code/internal/pkg/state/diff/format"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

func Option() diff.Option {
	return diff.WithCmpOption(diff.OnlyOnceTransformer("orchestration", func(v *model.ObjectNode) interface{} {
		if orchestration, ok := v.Object.(*model.Orchestration); ok {
			return newFormatter().format(orchestration, v.Children)
		}
		return v
	}))
}

type formatter struct {
	builder      strings.Builder
	placeholders format.Placeholders
}

func newFormatter() *formatter {
	return &formatter{placeholders: format.NewPlaceholders()}
}

func (f *formatter) format(_ *model.Orchestration, children model.ObjectChildren) format.ValueWithPlaceholders {
	f.builder.Reset()
	for _, phaseRaw := range children.Get(model.PhaseKind) {
		f.formatPhase(phaseRaw.Object.(*model.Phase), phaseRaw.Children)
	}
	return format.ValueWithPlaceholders{
		Value:        strings.TrimRight(f.builder.String(), "\n"),
		Placeholders: f.placeholders,
	}
}

func (f *formatter) formatPhase(phase *model.Phase, children model.ObjectChildren) {
	// Name
	f.builder.WriteString(fmt.Sprintf("# %s %s\n", phase.ObjectId(), phase.Name))

	// Depends on
	f.formatDependsOn(phase.DependsOn)

	// Content
	f.formatContent(phase.Content)

	// Tasks
	for _, taskRaw := range children.Get(model.TaskKind) {
		f.formatTask(taskRaw.Object.(*model.Task))
	}
}

func (f *formatter) formatTask(task *model.Task) {
	// Name
	f.builder.WriteString(fmt.Sprintf("## %s %s\n", task.ObjectId(), task.Name))

	// The target path is represented by a unique placeholder, which is replaced during formatting.
	f.builder.WriteString(fmt.Sprintf(">> %s\n", f.pathPlaceholder(task.TargetConfigKey())))

	// Content
	f.formatContent(task.Content)
}

func (f *formatter) formatDependsOn(dependsOnKeys []model.PhaseKey) {
	var dependsOn []string
	for _, dependsOnKey := range dependsOnKeys {
		dependsOn = append(dependsOn, dependsOnKey.ObjectId())
	}
	f.builder.WriteString(fmt.Sprintf("depends on phases: [%s]\n", strings.Join(dependsOn, `, `)))
}

func (f *formatter) formatContent(content *orderedmap.OrderedMap) {
	f.builder.WriteString(json.MustEncodeString(content, true))
}

func (f *formatter) pathPlaceholder(key model.Key) format.InternalValue {
	placeholder := format.NewPlaceholder("path:" + key.LogicPath())
	f.placeholders.Add("", func(f format.Formatter) string {
		if path, found := f.KeyFsPath(key); found {
			return path
		} else {
			return key.LogicPath()
		}
	})
	return placeholder
}
