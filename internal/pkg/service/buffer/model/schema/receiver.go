package schema

import (
	"strconv"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/model"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type receivers = PrefixT[model.Receiver]

type Receivers struct {
	receivers
}

type ReceiversInProject struct {
	receivers
}

func (v ConfigsRoot) Receivers() Receivers {
	return Receivers{receivers: NewTypedPrefix[model.Receiver](
		v.prefix.Add("receiver"),
		v.schema.serialization,
	)}
}

func (v Receivers) InProject(projectID int) ReceiversInProject {
	if projectID == 0 {
		panic(errors.New("receiver projectID cannot be empty"))
	}
	return ReceiversInProject{receivers: v.receivers.Add(strconv.Itoa(projectID))}
}

func (v ReceiversInProject) ID(receiverID string) KeyT[model.Receiver] {
	if receiverID == "" {
		panic(errors.New("receiver receiverID cannot be empty"))
	}
	return v.receivers.Key(receiverID)
}
