package repository

import (
	"fmt"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	// rollupSumKey contains the sum of all statistics from the object children that were deleted.
	rollupSumKey = "_sum"
	// rollupResetKey contains the sum of all statistics from the object children that are ignored.
	// ~ char is important, in alphabetical order, the reset will always be at the end - after the statistics to which it belongs.
	// We are using uint64 types, so we have to prevent underflow - the reset must be subtracted at the end.
	rollupResetKey = "~reset"
)

type (
	schema struct {
		etcdop.PrefixT[statistics.Value]
	}
	schemaInLevel  schema
	schemaInObject schema
)

func newSchema(s *serde.Serde) schema {
	return schema{PrefixT: etcdop.NewTypedPrefix[statistics.Value]("storage/stats", s)}
}

func (s schema) InLevel(l model.Level) schemaInLevel {
	switch l {
	case model.LevelLocal, model.LevelStaging, model.LevelTarget:
		return schemaInLevel{PrefixT: s.Add(l.String())}
	default:
		panic(errors.Errorf(`unexpected storage level "%v"`, l))
	}
}

// InParentOf returns prefix of the parent object, it is used as schemaInLevel.InParentOf(...).Sum().
func (v schemaInLevel) InParentOf(k fmt.Stringer) schemaInObject {
	switch k := k.(type) {
	case key.BranchKey:
		return v.inObject(k.ProjectID)
	case key.SourceKey:
		return v.inObject(k.BranchKey)
	case key.SinkKey:
		return v.inObject(k.SourceKey)
	case model.FileKey:
		return v.inObject(k.SinkKey)
	case model.SliceKey:
		return v.inObject(k.FileKey)
	default:
		panic(errors.Errorf(`unexpected object key "%T"`, k))
	}
}

func (v schemaInLevel) InObject(k fmt.Stringer) schemaInObject {
	switch k.(type) {
	case keboola.ProjectID, key.BranchKey, key.SourceKey, key.SinkKey, model.FileKey, model.SliceKey:
		return v.inObject(k)
	default:
		panic(errors.Errorf(`unexpected object key "%T"`, k))
	}
}

func (v schemaInLevel) InProject(projectID keboola.ProjectID) schemaInObject {
	return v.inObject(projectID)
}

func (v schemaInLevel) InBranch(k key.BranchKey) schemaInObject {
	return v.inObject(k)
}

func (v schemaInLevel) InSource(k key.SourceKey) schemaInObject {
	return v.inObject(k)
}

func (v schemaInLevel) InSink(k key.SinkKey) schemaInObject {
	return v.inObject(k)
}

func (v schemaInLevel) InFile(k model.FileKey) schemaInObject {
	return v.inObject(k)
}

func (v schemaInLevel) InSlice(k model.SliceKey) schemaInObject {
	return v.inObject(k)
}

func (v schemaInLevel) InSliceSourceNode(k model.SliceKey, nodeID string) etcdop.KeyT[statistics.Value] {
	return v.inObject(k).Key(nodeID)
}

func (v schemaInLevel) inObject(objectKey fmt.Stringer) schemaInObject {
	return schemaInObject{PrefixT: v.Add(objectKey.String())}
}

func (v schemaInObject) Sum() etcdop.KeyT[statistics.Value] {
	return v.Key(rollupSumKey)
}

func (v schemaInObject) Reset() etcdop.KeyT[statistics.Value] {
	return v.Key(rollupResetKey)
}
