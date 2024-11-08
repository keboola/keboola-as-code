package schema

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	keboolasink "github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/type/tablesink/keboola"
)

type (
	// Job is an etcd prefix that stores all Keboola-specific data we need for job polling.
	Job struct {
		etcdop.PrefixT[keboolasink.Job]
	}
)

func forJob(s *serde.Serde) Job {
	return Job{PrefixT: etcdop.NewTypedPrefix[keboolasink.Job]("storage/keboola/job", s)}
}

func (v Job) ForJob(k key.JobKey) etcdop.KeyT[keboolasink.Job] {
	return v.PrefixT.Key(k.String())
}
