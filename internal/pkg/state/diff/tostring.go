package diff

import (
	"fmt"
	"strings"

	"github.com/spf13/cast"

	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local/naming"
)

func phaseToString(p model.Phase, naming *naming.Registry) string {
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("#  %03d %s\n", p.Index+1, p.Name))

	var dependsOn []string
	for _, dependsOnKey := range p.DependsOn {
		dependsOn = append(dependsOn, cast.ToString(dependsOnKey.Index+1))
	}

	builder.WriteString(fmt.Sprintf("depends on phases: [%s]\n", strings.Join(dependsOn, `, `)))
	builder.WriteString(fmt.Sprintf(json.MustEncodeString(p.Content, true)))
	for _, task := range p.Tasks {
		builder.WriteString(taskToString(*task, naming))
	}
	return strings.TrimRight(builder.String(), "\n")
}

func taskToString(t model.Task, naming *naming.Registry) string {
	targetConfigKey := t.TargetConfigKey()
	targetConfigDesc := targetConfigKey.String()
	if targetConfigPath, found := naming.PathByKey(targetConfigKey); found {
		targetConfigDesc = targetConfigPath.RelativePath()
	}

	return fmt.Sprintf(
		"## %03d %s\n>> %s\n%s",
		t.Index+1,
		t.Name,
		targetConfigDesc,
		json.MustEncodeString(t.Content, true),
	)
}
