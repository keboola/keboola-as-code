package schema

import (
	storeKey "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type receivers = PrefixT[model.ReceiverBase]

type Receivers struct {
	receivers
}

type ReceiversInProject struct {
	receivers
}

func (v ConfigsRoot) Receivers() Receivers {
	return Receivers{receivers: NewTypedPrefix[model.ReceiverBase](
		v.prefix.Add("receiver"),
		v.schema.serde,
	)}
}

func (v Receivers) ByKey(k storeKey.ReceiverKey) KeyT[model.ReceiverBase] {
	if k.ReceiverID == "" {
		panic(errors.New("receiver receiverID cannot be empty"))
	}
	return v.InProject(k.ProjectID).Key(k.ReceiverID.String())
}

func (v Receivers) InProject(projectID storeKey.ProjectID) ReceiversInProject {
	if projectID == 0 {
		panic(errors.New("receiver projectID cannot be empty"))
	}
	return ReceiversInProject{receivers: v.receivers.Add(projectID.String())}
}
