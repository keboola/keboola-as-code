package schema

import (
	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/type/tablesink/keboola/bridge/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type (
	// Job is an etcd prefix that stores all Keboola-specific data we need for job polling.
	Job struct {
		etcdop.PrefixT[model.Job]
	}
)

func New(s *serde.Serde) Job {
	return Job{PrefixT: etcdop.NewTypedPrefix[model.Job]("storage/keboola/job", s)}
}

func (j Job) ForSink(k model.JobKey) etcdop.KeyT[model.Job] {
	return j.Key(k.String())
}

func (j Job) In(objectKey any) etcdop.PrefixT[model.Job] {
	switch k := objectKey.(type) {
	case keboola.ProjectID:
		return j.InProject(k)
	case key.BranchKey:
		return j.InBranch(k)
	case key.SourceKey:
		return j.InSource(k)
	case key.SinkKey:
		return j.InSink(k)
	default:
		panic(errors.Errorf(`unexpected Job parent key type "%T"`, objectKey))
	}
}

func (j Job) InProject(k keboola.ProjectID) etcdop.PrefixT[model.Job] {
	return j.Add(k.String())
}

func (j Job) InBranch(k key.BranchKey) etcdop.PrefixT[model.Job] {
	return j.Add(k.String())
}

func (j Job) InSource(k key.SourceKey) etcdop.PrefixT[model.Job] {
	return j.Add(k.String())
}

func (j Job) InSink(k key.SinkKey) etcdop.PrefixT[model.Job] {
	return j.Add(k.String())
}

func (j Job) ByKey(k model.JobKey) etcdop.KeyT[model.Job] {
	return j.Key(k.String())
}
