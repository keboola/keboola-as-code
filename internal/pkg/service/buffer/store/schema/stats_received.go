package schema

import (
	storeKey "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
)

type sliceStats = PrefixT[model.SliceStats]

type ReceivedStats struct {
	sliceStats
}

type ReceivedStatsByReceiver struct {
	sliceStats
}

type ReceivedStatsByExport struct {
	sliceStats
}

type ReceivedStatsByFile struct {
	sliceStats
}

type ReceivedStatsBySlice struct {
	sliceStats
}

type ReceivedStatsBySliceAndNode struct {
	sliceStats
}

func (v *Schema) ReceivedStats() ReceivedStats {
	return ReceivedStats{
		sliceStats: NewTypedPrefix[model.SliceStats]("stats/received", v.serde),
	}
}

func (v ReceivedStats) InReceiver(k storeKey.ReceiverKey) ReceivedStatsByReceiver {
	return ReceivedStatsByReceiver{sliceStats: v.sliceStats.Add(k.String())}
}

func (v ReceivedStats) InExport(k storeKey.ExportKey) ReceivedStatsByExport {
	return ReceivedStatsByExport{sliceStats: v.Add(k.String())}
}

func (v ReceivedStats) InFile(k storeKey.FileKey) ReceivedStatsByFile {
	return ReceivedStatsByFile{sliceStats: v.Add(k.String())}
}

func (v ReceivedStats) InSlice(k storeKey.SliceKey) ReceivedStatsBySlice {
	return ReceivedStatsBySlice{sliceStats: v.Add(k.String())}
}

func (v ReceivedStatsBySlice) ByNodeID(nodeID string) KeyT[model.SliceStats] {
	return v.sliceStats.Key(nodeID)
}
