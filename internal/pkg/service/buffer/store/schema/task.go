package schema

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	commonKey "github.com/keboola/keboola-as-code/internal/pkg/service/common/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/store/model"
	taskKey "github.com/keboola/keboola-as-code/internal/pkg/service/common/task/key"
)

type tasks = PrefixT[model.Task]

type TasksRoot struct {
	tasks
}

type TasksInProject struct {
	tasks
}

type TasksInReceiver struct {
	tasks
}

type TasksInExport struct {
	tasks
}

func (v *Schema) Tasks() TasksRoot {
	return TasksRoot{tasks: NewTypedPrefix[model.Task]("task", v.serde)}
}

func (v TasksRoot) InProject(projectID commonKey.ProjectID) TasksInProject {
	return TasksInProject{tasks: v.tasks.Add(projectID.String())}
}

func (v TasksRoot) InReceiver(k key.ReceiverKey) TasksInReceiver {
	return TasksInReceiver{tasks: v.InProject(k.ProjectID).tasks.Add(k.ReceiverID.String())}
}

func (v TasksRoot) InExport(k key.ExportKey) TasksInExport {
	return TasksInExport{tasks: v.InReceiver(k.ReceiverKey).tasks.Add(k.ExportID.String())}
}

func (v TasksInProject) ByID(id taskKey.ID) KeyT[model.Task] {
	return v.tasks.Key(id.String())
}

func (v TasksRoot) ByKey(k taskKey.Key) KeyT[model.Task] {
	return v.InProject(k.ProjectID).ByID(k.TaskID)
}
