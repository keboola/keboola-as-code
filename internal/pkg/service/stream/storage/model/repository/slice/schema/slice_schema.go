package schema

import (
	"fmt"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type (
	// Slice is an etcd prefix that stores all slice entities.
	Slice struct{ etcdop.PrefixT[model.Slice] }
	// SliceInLevel - see Slice.AllLevels and Slice.InLevel methods.
	// The entity is stored in 2 copies, under "All" prefix and "InLevel" prefix.
	// - "All" prefix is used for classic CRUD operations.
	// - "InLevel" prefix is used for effective watching of the storage level.
	SliceInLevel  Slice
	SliceInObject Slice
)

func New(s *serde.Serde) Slice {
	return Slice{PrefixT: etcdop.NewTypedPrefix[model.Slice]("storage/slice", s)}
}

func (s Slice) AllLevels() SliceInLevel {
	return SliceInLevel{PrefixT: s.Add("all")}
}

func (s Slice) InLevel(l model.Level) SliceInLevel {
	switch l {
	case model.LevelLocal, model.LevelStaging, model.LevelTarget:
		return SliceInLevel{PrefixT: s.PrefixT.Add("level").Add(l.String())}
	default:
		panic(errors.Errorf(`unexpected storage level "%v"`, l))
	}
}

func (v SliceInLevel) ByKey(k model.SliceKey) etcdop.KeyT[model.Slice] {
	return v.Key(k.String())
}

func (v SliceInLevel) InObject(k fmt.Stringer) SliceInObject {
	switch k.(type) {
	case keboola.ProjectID, key.BranchKey, key.SourceKey, key.SinkKey, model.FileKey, model.FileVolumeKey:
		return v.inObject(k)
	default:
		panic(errors.Errorf(`unexpected object key "%T"`, k))
	}
}

func (v SliceInLevel) InProject(projectID keboola.ProjectID) SliceInObject {
	return v.inObject(projectID)
}

func (v SliceInLevel) InBranch(k key.BranchKey) SliceInObject {
	return v.inObject(k)
}

func (v SliceInLevel) InSource(k key.SourceKey) SliceInObject {
	return v.inObject(k)
}

func (v SliceInLevel) InSink(k key.SinkKey) SliceInObject {
	return v.inObject(k)
}

func (v SliceInLevel) InFile(k model.FileKey) SliceInObject {
	return v.inObject(k)
}

func (v SliceInLevel) InFileVolume(k model.FileVolumeKey) SliceInObject {
	return v.inObject(k)
}

func (v SliceInLevel) inObject(objectKey fmt.Stringer) SliceInObject {
	return SliceInObject{PrefixT: v.Add(objectKey.String())}
}
