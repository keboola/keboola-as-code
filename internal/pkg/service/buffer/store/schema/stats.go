package schema

import (
	"fmt"

	"github.com/keboola/go-client/pkg/keboola"

	storeKey "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/slicestate"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	// statsNodesSumKey contains sum of statistics for a slice from all API nodes,
	// the value is calculated when the slice is closed and all partial values are deleted.
	statsNodesSumKey = "_nodes_sum"
	// statsReduceSumKey contains sum of statistics of all old slices deleted by cleanup.
	statsReduceSumKey = "_reduce_sum"
)

type sliceStats = PrefixT[model.Stats]

type Stats struct {
	sliceStats
}

type OpenedSliceAPINodeStats struct {
	sliceStats
}

type SlicesInStateStats struct {
	sliceStats
	state slicestate.State
}

type ExportStats struct {
	sliceStats
	state slicestate.State
}

type SliceStats struct {
	sliceStats
	state slicestate.State
}

func StatsKeyFrom(s slicestate.State) string {
	// During the closing of a slice, the API nodes are switched from the old to the new slice.
	// In this phase, each API node writes statistics to its key, which cannot be changed,
	// because the API node is notified about the changes later.
	// This problem does not exist with other states, because then the slice is no longer written
	// to and the statistics are not changed.
	// Therefore, if the slice is in state Writing or Closing, the statistics are always stored under Writing key.
	if s == slicestate.Closing {
		s = slicestate.Writing
	}
	return s.String()
}

func (v *Schema) SliceStats() Stats {
	return Stats{
		sliceStats: NewTypedPrefix[model.Stats]("stats/slice", v.serde),
	}
}

func (v Stats) InState(s slicestate.State) SlicesInStateStats {
	return SlicesInStateStats{state: s, sliceStats: v.sliceStats.Add(StatsKeyFrom(s))}
}

func (v SlicesInStateStats) InObject(objectKey fmt.Stringer) PrefixT[model.Stats] {
	return v.sliceStats.Add(objectKey.String()).PrefixT()
}

func (v SlicesInStateStats) InProject(projectID keboola.ProjectID) PrefixT[model.Stats] {
	return v.InObject(projectID)
}

func (v SlicesInStateStats) InReceiver(k storeKey.ReceiverKey) PrefixT[model.Stats] {
	return v.InObject(k)
}

func (v SlicesInStateStats) InExport(k storeKey.ExportKey) ExportStats {
	return ExportStats{state: v.state, sliceStats: v.sliceStats.Add(k.String())}
}

func (v SlicesInStateStats) InFile(k storeKey.FileKey) PrefixT[model.Stats] {
	return v.InObject(k)
}

func (v SlicesInStateStats) InSlice(k storeKey.SliceKey) SliceStats {
	return SliceStats{state: v.state, sliceStats: v.sliceStats.Add(k.String())}
}

// ReduceSum key contains sum of statistics of all old slices deleted by cleanup.
func (v ExportStats) ReduceSum() KeyT[model.Stats] {
	if v.state != slicestate.Imported {
		panic(errors.Errorf(`ReduceSum method can be used only for state "%s", given "%s"`, slicestate.Imported, v.state))
	}
	return v.sliceStats.Key(statsReduceSumKey)
}

// NodeID returns key to store slice statistics per API node, it is applicable on for slicestate.Writing.
func (v SliceStats) NodeID(nodeID string) KeyT[model.Stats] {
	if v.state != slicestate.Writing {
		panic(errors.Errorf(`NodeID method can be used only for state "%s", given "%s"`, slicestate.Writing, v.state))
	}
	if nodeID == "" {
		panic(errors.New("node ID cannot be empty"))
	}
	return v.sliceStats.Key(nodeID)
}

func (v SliceStats) AllNodesSum() KeyT[model.Stats] {
	if v.state == slicestate.Writing || v.state == slicestate.Closing {
		panic(errors.Errorf(`AllNodesSum method cannot be used for state "%s" or "%s"`, slicestate.Writing, slicestate.Closing))
	}
	return v.sliceStats.Key(statsNodesSumKey)
}
