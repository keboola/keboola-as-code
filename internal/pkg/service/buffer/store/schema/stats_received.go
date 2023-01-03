package schema

import (
	storeKey "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
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
	if k.ProjectID == 0 {
		panic(errors.New("stats projectID cannot be empty"))
	}
	if k.ReceiverID == "" {
		panic(errors.New("stats receiverID cannot be empty"))
	}
	return ReceivedStatsByReceiver{
		sliceStats: v.sliceStats.
			Add(k.ProjectID.String()).
			Add(k.ReceiverID.String()),
	}
}

func (v ReceivedStats) InExport(k storeKey.ExportKey) ReceivedStatsByExport {
	if k.ExportID == "" {
		panic(errors.New("stats receiverID cannot be empty"))
	}
	return ReceivedStatsByExport{
		sliceStats: v.InReceiver(k.ReceiverKey).sliceStats.Add(k.ExportID.String()),
	}
}

func (v ReceivedStats) InFile(k storeKey.FileKey) ReceivedStatsByFile {
	if k.ExportID == "" {
		panic(errors.New("stats exportID cannot be empty"))
	}
	if k.FileID.IsZero() {
		panic(errors.New("stats fileID cannot be empty"))
	}
	return ReceivedStatsByFile{
		sliceStats: v.InExport(k.ExportKey).sliceStats.Add(k.FileID.String()),
	}
}

func (v ReceivedStats) InSlice(k storeKey.SliceKey) ReceivedStatsBySlice {
	if k.SliceID.IsZero() {
		panic(errors.New("stats sliceID cannot be empty"))
	}
	return ReceivedStatsBySlice{
		sliceStats: v.InFile(k.FileKey).sliceStats.Add(k.SliceID.String()),
	}
}

func (v ReceivedStatsBySlice) ByNodeID(nodeID string) KeyT[model.SliceStats] {
	if nodeID == "" {
		panic(errors.New("stats nodeID cannot be empty"))
	}
	return v.sliceStats.Key(nodeID)
}
