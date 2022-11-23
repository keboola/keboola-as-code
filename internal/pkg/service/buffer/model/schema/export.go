package schema

import (
	"strconv"

	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
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
	if projectID == 0 {
		panic(errors.New("export projectID cannot be empty"))
	}
	return ExportsInProject{prefix: v.prefix.Add(strconv.Itoa(projectID))}
}

func (v ExportsInProject) InReceiver(receiverID string) ExportsInReceiver {
	if receiverID == "" {
		panic(errors.New("export receiverID cannot be empty"))
	}
	return ExportsInReceiver{prefix: v.prefix.Add(receiverID)}
}

func (v ExportsInReceiver) ID(exportID string) Key {
	if exportID == "" {
		panic(errors.New("export exportID cannot be empty"))
	}
	return v.prefix.Key(exportID)
}
