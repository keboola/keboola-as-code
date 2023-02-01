package schema

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
)

type tasks = PrefixT[model.Task]

type TasksRoot struct {
	tasks
}

type TasksByProject struct {
	tasks
}

type TasksByReceiver struct {
	tasks
}

type TasksByExport struct {
	tasks
}

func (v *Schema) Tasks() TasksRoot {
	return TasksRoot{tasks: NewTypedPrefix[model.Task]("task", v.serde)}
}

func (v TasksRoot) InProject(projectID key.ProjectID) TasksByProject {
	return TasksByProject{tasks: v.tasks.Add(projectID.String())}
}

func (v TasksRoot) InReceiver(k key.ReceiverKey) TasksByReceiver {
	return TasksByReceiver{tasks: v.tasks.Add(k.String())}
}

func (v TasksRoot) InExport(k key.ExportKey) TasksByExport {
	return TasksByExport{tasks: v.tasks.Add(k.String())}
}

func (v TasksRoot) ByKey(k key.TaskKey) KeyT[model.Task] {
	return v.tasks.Key(k.String())
}
