// Package distlock provides distributed locks.
package distlock

import (
	"context"
	"sync"
	"time"

	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
)

const lockEtcdPrefix = "lock"

type Provider struct {
	logger  log.Logger
	session *etcdop.Session
}

type Config struct {
	// GrantTimeout it the maximum time to wait for creating a new session.
	GrantTimeout time.Duration `configKey:"grantTimeout" configUsage:"The maximum time to wait for creating a new session." validate:"required,minDuration=1s,maxDuration=1m"`
	// TTLSeconds configures the number seconds after which all locks are automatically released if an outage occurs.
	TTLSeconds int `configKey:"ttlSeconds" configUsage:"Seconds after which all locks are automatically released if an outage occurs." validate:"required,min=1,max=30"`
}

type dependencies interface {
	Logger() log.Logger
	Process() *servicectx.Process
	EtcdClient() *etcd.Client
}

func NewConfig() Config {
	return Config{
		GrantTimeout: 5 * time.Second,
		TTLSeconds:   15,
	}
}

func NewProvider(cfg Config, d dependencies) (*Provider, error) {
	p := &Provider{}
	p.logger = d.Logger().WithComponent("distribution.mutex.provider")

	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	d.Process().OnShutdown(func(_ context.Context) {
		p.logger.Info(ctx, "received shutdown request")
		cancel()
		wg.Wait()
		p.logger.Info(ctx, "shutdown done")
	})

	var err error
	p.session, err = etcdop.
		NewSessionBuilder().
		WithGrantTimeout(cfg.GrantTimeout).
		WithTTLSeconds(cfg.TTLSeconds).
		StartOrErr(ctx, wg, p.logger, d.EtcdClient())
	if err != nil {
		return nil, err
	}

	return p, nil
}

func (p *Provider) NewMutex(name string) *etcdop.Mutex {
	return p.session.NewMutex(lockEtcdPrefix + "/" + name)
}
