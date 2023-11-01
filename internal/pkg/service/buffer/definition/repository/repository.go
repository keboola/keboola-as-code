package repository

import (
	"github.com/benbjohnson/clock"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

type dependencies interface {
	Telemetry() telemetry.Telemetry
	Clock() clock.Clock
	EtcdClient() *etcd.Client
	EtcdSerde() *serde.Serde
}

type Repository struct {
	branch *BranchRepository
	source *SourceRepository
	sink   *SinkRepository
}

func NewRepository(d dependencies) *Repository {
	r := &Repository{}
	r.branch = newBranchRepository(d, r)
	r.source = newSourceRepository(d, r)
	r.sink = newSinkRepository(d, r)
	return r
}

func (r *Repository) Branch() *BranchRepository {
	return r.branch
}

func (r *Repository) Source() *SourceRepository {
	return r.source
}

func (r *Repository) Sink() *SinkRepository {
	return r.sink
}
