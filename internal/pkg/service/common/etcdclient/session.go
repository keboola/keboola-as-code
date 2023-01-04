package etcdclient

import (
	"time"

	etcd "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
)

func CreateConcurrencySession(logger log.Logger, proc *servicectx.Process, client *etcd.Client, ttlSeconds int) (session *concurrency.Session, err error) {
	startTime := time.Now()
	logger = logger.AddPrefix("[etcd-session]")
	logger.Infof(`creating etcd session`)

	session, err = concurrency.NewSession(client, concurrency.WithTTL(ttlSeconds))
	if err != nil {
		return nil, err
	}

	proc.OnShutdown(func() {
		startTime := time.Now()
		logger.Info("closing etcd session")
		if err := session.Close(); err != nil {
			logger.Warnf("cannot close etcd session: %s", err)
		} else {
			logger.Infof("closed etcd session | %s", time.Since(startTime))
		}
	})

	logger.Infof("created etcd session | %s", time.Since(startTime))
	return session, nil
}
