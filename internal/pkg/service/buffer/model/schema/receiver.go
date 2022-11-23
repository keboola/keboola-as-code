package schema

import (
	"strconv"

	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
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
	if projectID == 0 {
		panic(errors.New("receiver projectID cannot be empty"))
	}
	return ReceiversInProject{prefix: v.prefix.Add(strconv.Itoa(projectID))}
}

func (v ReceiversInProject) ID(receiverID string) Key {
	if receiverID == "" {
		panic(errors.New("receiver receiverID cannot be empty"))
	}
	return v.prefix.Key(receiverID)
}
