package repository

import (
	"fmt"

	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/statistics"
	storeKey "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	// sliceValueKey contains a slice statistics, it is the lowest level.
	sliceValueKey = "value"
	// rollupSumKey contains the sum of all statistics from the object children that were deleted.
	rollupSumKey = "_sum"
)

type prefix = PrefixT[statistics.Value]

type SchemaRoot struct {
	prefix
}

type SchemaInLevel struct {
	prefix
}

type SchemaInObject struct {
	prefix
}

func newSchema(s *serde.Serde) SchemaRoot {
	return SchemaRoot{prefix: NewTypedPrefix[statistics.Value]("storage/stats", s)}
}

func (s SchemaRoot) InLevel(level storage.Level) SchemaInLevel {
	switch level {
	case storage.LevelLocal, storage.LevelStaging, storage.LevelTarget:
		return SchemaInLevel{prefix: s.prefix.Add(level.String())}
	default:
		panic(errors.Errorf(`unexpected storage level "%v"`, level))
	}
}

// InParentOf returns prefix of the parent object, it is used as SchemaInLevel.InParentOf(...).Sum().
func (v SchemaInLevel) InParentOf(k fmt.Stringer) SchemaInObject {
	switch k := k.(type) {
	case storeKey.ReceiverKey:
		return v.inObject(k.ProjectID)
	case storeKey.ExportKey:
		return v.inObject(k.ReceiverKey)
	case storage.FileKey:
		return v.inObject(k.ExportKey)
	case storage.SliceKey:
		return v.inObject(k.FileKey)
	default:
		panic(errors.Errorf(`unexpected object key "%T"`, k))
	}
}

func (v SchemaInLevel) InObject(k fmt.Stringer) SchemaInObject {
	switch k.(type) {
	case keboola.ProjectID, storeKey.ReceiverKey, storeKey.ExportKey, storage.FileKey, storage.SliceKey:
		return v.inObject(k)
	default:
		panic(errors.Errorf(`unexpected object key "%T"`, k))
	}
}

func (v SchemaInLevel) InProject(projectID keboola.ProjectID) SchemaInObject {
	return v.inObject(projectID)
}

func (v SchemaInLevel) InReceiver(k storeKey.ReceiverKey) SchemaInObject {
	return v.inObject(k)
}

func (v SchemaInLevel) InExport(k storeKey.ExportKey) SchemaInObject {
	return v.inObject(k)
}

func (v SchemaInLevel) InFile(k storage.FileKey) SchemaInObject {
	return v.inObject(k)
}

func (v SchemaInLevel) InSlice(k storage.SliceKey) KeyT[statistics.Value] {
	return v.inObject(k).Key(sliceValueKey)
}

func (v SchemaInLevel) inObject(objectKey fmt.Stringer) SchemaInObject {
	return SchemaInObject{prefix: v.prefix.Add(objectKey.String())}
}

func (v SchemaInObject) Sum() KeyT[statistics.Value] {
	return v.prefix.Key(rollupSumKey)
}
