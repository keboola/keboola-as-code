package transformer

//
//func (t *Transformer) orchestrationToString(orchestration *model.ObjectNode) string {
//	var builder strings.Builder
//
//	// Phases
//	for _, phaseRaw := range orchestration.Get(model.PhaseKind) {
//		phase := phaseRaw.Object.(*model.Phase)
//		builder.WriteString(fmt.Sprintf("#  %03d %s\n", phase.PhaseIndex+1, phase.Name))
//
//		var dependsOn []string
//		for _, dependsOnKey := range phase.DependsOn {
//			dependsOn = append(dependsOn, cast.ToString(dependsOnKey.PhaseIndex+1))
//		}
//
//		builder.WriteString(fmt.Sprintf("depends on phases: [%s]\n", strings.Join(dependsOn, `, `)))
//		builder.WriteString(json.MustEncodeString(phase.Content, true))
//
//		// Tasks
//		for _, taskRaw := range phaseRaw.Get(model.TaskKind) {
//			task := taskRaw.Object.(*model.Task)
//
//			// Print target config path if possible
//			targetConfigKey := task.TargetConfigKey()
//			targetConfigDesc := targetConfigKey.String()
//			if targetConfigPath, ok := t.naming.PathByKey(targetConfigKey); ok {
//				targetConfigDesc = targetConfigPath.String()
//			}
//
//			builder.WriteString(fmt.Sprintf(
//				"## %03d %s\n>> %s\n%s\n",
//				task.TaskIndex+1,
//				task.Name,
//				targetConfigDesc,
//				json.MustEncodeString(task.Content, true),
//			))
//		}
//	}
//
//	return strings.Trim(builder.String(), "\n")
//}
