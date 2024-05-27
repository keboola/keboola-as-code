package schema

import (
	"fmt"

	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type (
	// File is an etcd prefix that stores all file entities.
	File struct{ etcdop.PrefixT[model.File] }
	// FileInLevel - see File.AllLevels and fileSchema.InLevel methods.
	// The entity is stored in 2 copies, under "All" prefix and "InLevel" prefix.
	// - "All" prefix is used for classic CRUD operations.
	// - "InLevel" prefix is used for effective watching of the storage level.
	FileInLevel  File
	FileInObject File
)

func New(s *serde.Serde) File {
	return File{PrefixT: etcdop.NewTypedPrefix[model.File]("storage/file", s)}
}

func (s File) AllLevels() FileInLevel {
	return FileInLevel{PrefixT: s.PrefixT.Add("all")}
}

func (s File) InLevel(l level.Level) FileInLevel {
	switch l {
	case level.Local, level.Staging, level.Target:
		return FileInLevel{PrefixT: s.PrefixT.Add("level").Add(l.String())}
	default:
		panic(errors.Errorf(`unexpected storage level "%v"`, l))
	}
}

func (v FileInLevel) ByKey(k model.FileKey) etcdop.KeyT[model.File] {
	return v.PrefixT.Key(k.String())
}

func (v FileInLevel) InObject(k fmt.Stringer) FileInObject {
	switch k.(type) {
	case keboola.ProjectID, key.BranchKey, key.SourceKey, key.SinkKey:
		return v.inObject(k)
	default:
		panic(errors.Errorf(`unexpected object key "%T"`, k))
	}
}

func (v FileInLevel) InProject(projectID keboola.ProjectID) FileInObject {
	return v.inObject(projectID)
}

func (v FileInLevel) InBranch(k key.BranchKey) FileInObject {
	return v.inObject(k)
}

func (v FileInLevel) InSource(k key.SourceKey) FileInObject {
	return v.inObject(k)
}

func (v FileInLevel) InSink(k key.SinkKey) FileInObject {
	return v.inObject(k)
}

func (v FileInLevel) inObject(objectKey fmt.Stringer) FileInObject {
	return FileInObject{PrefixT: v.PrefixT.Add(objectKey.String())}
}
