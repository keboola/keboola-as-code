package diff

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local/naming"
)

func codeToString(c *model.Code, _ *naming.Registry) *string {
	if c == nil {
		return nil
	}

	str := fmt.Sprintf("## %s\n%s", c.Name, c.Scripts.String(c.ComponentId()))
	return &str
}

//func phaseToString(p *model.Phase, naming *naming.Registry) *string {
//	if p == nil {
//		return nil
//	}
//
//	var builder strings.Builder
//	builder.WriteString(fmt.Sprintf("#  %03d %s\n", p.Index+1, p.Name))
//
//	var dependsOn []string
//	for _, dependsOnKey := range p.DependsOn {
//		dependsOn = append(dependsOn, cast.ToString(dependsOnKey.Index+1))
//	}
//
//	builder.WriteString(fmt.Sprintf("depends on phases: [%s]\n", strings.Join(dependsOn, `, `)))
//	builder.WriteString(fmt.Sprintf(json.MustEncodeString(p.Content, true)))
//	for _, task := range p.Tasks {
//		taskStr := taskToString(task, naming)
//		if taskStr != nil {
//			builder.WriteString(*taskStr)
//		}
//	}
//
//	str := strings.TrimRight(builder.String(), "\n")
//	return &str
//}

func taskToString(t *model.Task, naming *naming.Registry) *string {
	if t == nil {
		return nil
	}

	targetConfigKey := t.TargetConfigKey()
	targetConfigDesc := targetConfigKey.String()
	if targetConfigPath, found := naming.PathByKey(targetConfigKey); found {
		targetConfigDesc = targetConfigPath.RelativePath()
	}

	str := fmt.Sprintf(
		"## %03d %s\n>> %s\n%s",
		t.Index+1,
		t.Name,
		targetConfigDesc,
		json.MustEncodeString(t.Content, true),
	)
	return &str
}

func sharedCodeRowTostring(c *model.SharedCodeRow, _ *naming.Registry) *string {
	if c == nil {
		return nil
	}

	str := c.Scripts.String(c.Target)
	return &str
}
