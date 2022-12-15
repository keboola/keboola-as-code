package schema

import (
	"strconv"

	storeKey "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type sliceStats = PrefixT[model.SliceStats]

type SliceStats struct {
	sliceStats
	schema *Schema
}

func (v *Schema) SliceStats() SliceStats {
	return SliceStats{
		sliceStats: NewTypedPrefix[model.SliceStats]("stats/received", v.serde),
		schema:     v,
	}
}

func (v SliceStats) ByKey(k storeKey.SliceStatsKey) KeyT[model.SliceStats] {
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
	if k.NodeID == "" {
		panic(errors.New("stats nodeID cannot be empty"))
	}
	return v.sliceStats.
		Add(strconv.Itoa(k.ProjectID)).
		Add(k.ReceiverID).
		Add(k.ExportID).
		Add(storeKey.FormatTime(k.FileID)).
		Add(storeKey.FormatTime(k.SliceID)).
		Key(k.NodeID)
}
