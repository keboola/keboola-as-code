package schema

import (
	storeKey "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	commonKey "github.com/keboola/keboola-as-code/internal/pkg/service/common/store/key"
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

func (v Receivers) InProject(projectID commonKey.ProjectID) ReceiversInProject {
	return ReceiversInProject{receivers: v.receivers.Add(projectID.String())}
}

func (v Receivers) ByKey(k storeKey.ReceiverKey) KeyT[model.ReceiverBase] {
	return v.Key(k.String())
}
