package repository

import (
	"fmt"

	"github.com/keboola/go-client/pkg/keboola"

	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	// sliceValueKey contains a slice statistics, it is the lowest level.
	sliceValueKey = "value"
	// rollupSumKey contains the sum of all statistics from the object children that were deleted.
	rollupSumKey = "_sum"
)

type (
	schema         struct{ PrefixT[statistics.Value] }
	schemaInLevel  schema
	schemaInObject schema
)

func newSchema(s *serde.Serde) schema {
	return schema{PrefixT: NewTypedPrefix[statistics.Value]("storage/stats", s)}
}

func (s schema) InLevel(l level.Level) schemaInLevel {
	switch l {
	case level.Local, level.Staging, level.Target:
		return schemaInLevel{PrefixT: s.PrefixT.Add(l.String())}
	default:
		panic(errors.Errorf(`unexpected storage level "%v"`, l))
	}
}

// InParentOf returns prefix of the parent object, it is used as schemaInLevel.InParentOf(...).Sum().
func (v schemaInLevel) InParentOf(k fmt.Stringer) schemaInObject {
	switch k := k.(type) {
	case BranchKey:
		return v.inObject(k.ProjectID)
	case SourceKey:
		return v.inObject(k.BranchKey)
	case SinkKey:
		return v.inObject(k.SourceKey)
	case FileKey:
		return v.inObject(k.SinkKey)
	case SliceKey:
		return v.inObject(k.FileKey)
	default:
		panic(errors.Errorf(`unexpected object key "%T"`, k))
	}
}

func (v schemaInLevel) InObject(k fmt.Stringer) schemaInObject {
	switch k.(type) {
	case keboola.ProjectID, BranchKey, SourceKey, SinkKey, FileKey, SliceKey:
		return v.inObject(k)
	default:
		panic(errors.Errorf(`unexpected object key "%T"`, k))
	}
}

func (v schemaInLevel) InProject(projectID keboola.ProjectID) schemaInObject {
	return v.inObject(projectID)
}

func (v schemaInLevel) InBranch(k BranchKey) schemaInObject {
	return v.inObject(k)
}

func (v schemaInLevel) InSource(k SourceKey) schemaInObject {
	return v.inObject(k)
}

func (v schemaInLevel) InSink(k SinkKey) schemaInObject {
	return v.inObject(k)
}

func (v schemaInLevel) InFile(k FileKey) schemaInObject {
	return v.inObject(k)
}

func (v schemaInLevel) InSlice(k SliceKey) KeyT[statistics.Value] {
	return v.inObject(k).Key(sliceValueKey)
}

func (v schemaInLevel) inObject(objectKey fmt.Stringer) schemaInObject {
	return schemaInObject{PrefixT: v.PrefixT.Add(objectKey.String())}
}

func (v schemaInObject) Sum() KeyT[statistics.Value] {
	return v.PrefixT.Key(rollupSumKey)
}
