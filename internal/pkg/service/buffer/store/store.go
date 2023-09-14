// Package store provides database operations for configuring receivers and exports
// and other backend operations.
package store

import (
	"github.com/benbjohnson/clock"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

type Store struct {
	clock     clock.Clock
	logger    log.Logger
	client    *etcd.Client
	telemetry telemetry.Telemetry
	schema    *schema.Schema
	stats     *statistics.Repository
}

type dependencies interface {
	Clock() clock.Clock
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
	Schema() *schema.Schema
	EtcdClient() *etcd.Client
	StatisticsRepository() *statistics.Repository
}

func New(d dependencies) *Store {
	return &Store{
		clock:     d.Clock(),
		logger:    d.Logger(),
		telemetry: d.Telemetry(),
		client:    d.EtcdClient(),
		schema:    d.Schema(),
		stats:     d.StatisticsRepository(),
	}
}
