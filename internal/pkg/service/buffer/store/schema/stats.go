package schema

import (
	storeKey "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type sliceStats = PrefixT[model.SliceStats]

type SliceStats struct {
	sliceStats
}

type SliceNodeStats struct {
	sliceStats
}

func (v *Schema) SliceStats() SliceStats {
	return SliceStats{
		sliceStats: NewTypedPrefix[model.SliceStats]("stats/received", v.serde),
	}
}

func (v SliceStats) InSlice(k storeKey.SliceKey) SliceNodeStats {
	if k.ProjectID == 0 {
		panic(errors.New("stats projectID cannot be empty"))
	}
	if k.ReceiverID == "" {
		panic(errors.New("stats receiverID cannot be empty"))
	}
	if k.ExportID == "" {
		panic(errors.New("stats exportID cannot be empty"))
	}
	if k.FileID.IsZero() {
		panic(errors.New("stats fileID cannot be empty"))
	}
	if k.SliceID.IsZero() {
		panic(errors.New("stats sliceID cannot be empty"))
	}
	return SliceNodeStats{
		sliceStats: v.sliceStats.
			Add(k.ProjectID.String()).
			Add(k.ReceiverID.String()).
			Add(k.ExportID.String()).
			Add(k.FileID.String()).
			Add(k.SliceID.String()),
	}
}

func (v SliceNodeStats) ByNodeID(nodeID string) KeyT[model.SliceStats] {
	if nodeID == "" {
		panic(errors.New("stats nodeID cannot be empty"))
	}
	return v.sliceStats.Key(nodeID)
}
