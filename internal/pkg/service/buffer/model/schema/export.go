package schema

import (
	"strconv"

	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
)

type Exports struct {
	prefix
}

type ExportsInProject struct {
	prefix
}

type ExportsInReceiver struct {
	prefix
}

func (v ConfigsRoot) Exports() Exports {
	return Exports{prefix: v.prefix + "export/"}
}

func (v Exports) InProject(projectID int) ExportsInProject {
	return ExportsInProject{prefix: v.prefix.Add(strconv.Itoa(projectID))}
}

func (v ExportsInProject) InReceiver(receiverID string) ExportsInReceiver {
	return ExportsInReceiver{prefix: v.prefix.Add(receiverID)}
}

func (v ExportsInReceiver) ID(exportID string) Key {
	return v.prefix.Key(exportID)
}
