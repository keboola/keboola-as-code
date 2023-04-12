package schema

import (
	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	commonKey "github.com/keboola/keboola-as-code/internal/pkg/service/common/store/key"
	taskModel "github.com/keboola/keboola-as-code/internal/pkg/service/common/task"
	taskKey "github.com/keboola/keboola-as-code/internal/pkg/service/common/task/key"
)

type tasks = PrefixT[taskModel.Model]

type TasksRoot struct {
	tasks
}

type TasksInProject struct {
	tasks
}

func (v *Schema) Tasks() TasksRoot {
	return TasksRoot{tasks: NewTypedPrefix[taskModel.Model]("task", v.serde)}
}

func (v TasksRoot) InProject(projectID commonKey.ProjectID) TasksInProject {
	return TasksInProject{tasks: v.tasks.Add(projectID.String())}
}

func (v TasksInProject) ByID(id taskKey.ID) KeyT[taskModel.Model] {
	return v.tasks.Key(id.String())
}

func (v TasksRoot) ByKey(k taskKey.Key) KeyT[taskModel.Model] {
	return v.InProject(k.ProjectID).ByID(k.TaskID)
}
