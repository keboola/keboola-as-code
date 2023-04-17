package schema

import (
	"github.com/keboola/go-client/pkg/keboola"

	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/task"
)

type tasks = PrefixT[task.Task]

type TasksRoot struct {
	tasks
}

type TasksInProject struct {
	tasks
}

func (v *Schema) Tasks() TasksRoot {
	return TasksRoot{tasks: NewTypedPrefix[task.Task]("task", v.serde)}
}

func (v TasksRoot) InProject(projectID keboola.ProjectID) TasksInProject {
	return TasksInProject{tasks: v.tasks.Add(projectID.String())}
}

func (v TasksInProject) ByID(id task.ID) KeyT[task.Task] {
	return v.tasks.Key(id.String())
}

func (v TasksRoot) ByKey(k task.Key) KeyT[task.Task] {
	return v.InProject(k.ProjectID).ByID(k.TaskID)
}
