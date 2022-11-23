package schema

import (
	"strconv"

	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
)

type Receivers struct {
	prefix
}

type ReceiversInProject struct {
	prefix
}

func (v ConfigsRoot) Receivers() Receivers {
	return Receivers{prefix: v.prefix + "receiver/"}
}

func (v Receivers) InProject(projectID int) ReceiversInProject {
	return ReceiversInProject{prefix: v.prefix.Add(strconv.Itoa(projectID))}
}

func (v ReceiversInProject) ID(receiverID string) Key {
	return v.prefix.Key(receiverID)
}
