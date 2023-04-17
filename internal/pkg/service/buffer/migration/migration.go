package migration

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/umisama/go-regexpcache"
	etcd "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/worker/distribution"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/iterator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	migrationLockKey = etcdop.Key("runtime/migration/20230418/lock")
	migrationDoneKey = etcdop.Key("runtime/migration/20230418/done")
	maxTxnKeys       = 40
	maxTxnSize       = 800 * datasize.KB
)

func APINode(logger log.Logger, client *etcd.Client) error {
	if err := waitForMigration(logger, client); err != nil {
		return err
	}

	return nil
}

func WorkerNode(logger log.Logger, client *etcd.Client, dist *distribution.Node) error {
	sess, err := concurrency.NewSession(client)
	if err != nil {
		return err
	}

	// Wait for all new nodes
	logger.Info("waiting 3 minutes")
	<-time.After(3 * time.Minute)

	// Only one worker should do migration
	if !dist.MustCheckIsOwner("migration") {
		return waitForMigration(logger, client)
	}
	ok, err := migrationLockKey.PutIfNotExists("locked", etcd.WithLease(sess.Lease())).Do(context.Background(), client)
	if err != nil {
		return err
	}
	if !ok {
		logger.Warn("migration is already running")
		return waitForMigration(logger, client)
	}

	return doMigration(logger, client, sess)
}

func doMigration(logger log.Logger, client *etcd.Client, sess *concurrency.Session) error {
	logger = logger.AddPrefix("[migration]")

	// Wait for rotation of all nodes
	logger.Info("starting ...")

	var txnKeys int
	var txnSize datasize.ByteSize
	var txn *op.TxnOp
	reset := func() {
		txnKeys = 0
		txnSize = 0
		txn = op.NewTxnOp()
	}
	reset()

	move := func(oldK, newK string, value []byte) {
		txn.Then(etcdop.Key(newK).Put(string(value)))
		txn.Then(etcdop.Key(oldK).Delete())
		txnKeys++
		txnSize += datasize.ByteSize(len(value))
		logger.Infof(`%s -> %s`, oldK, newK)
	}

	flush := func() error {
		ok, err := txn.Do(context.Background(), client)
		if err != nil {
			return err
		}
		if !ok.Succeeded {
			return errors.Errorf("")
		}
		reset()
		return nil
	}

	newKey := func(m []string) string {
		projectID, err := strconv.Atoi(m[2])
		if err != nil {
			panic(err)
		}
		return m[1] + strconv.Itoa(projectID) + m[3]
	}

	onKey := func(kv *op.KeyValue, header *iterator.Header) error {
		if txnKeys >= maxTxnKeys || (txnSize+datasize.ByteSize(len(kv.Value))) >= maxTxnSize {
			return flush()
		}

		k := string(kv.Key)
		switch {
		case strings.HasPrefix(k, "config/receiver/"):
			m := regexpcache.MustCompile(`^(config/receiver/)(0[0-9]+)(/.*)$`).FindStringSubmatch(k)
			if m != nil {
				move(k, newKey(m), kv.Value)
			} else {
				logger.Infof(`ignored "%s"`, k)
			}
		case strings.HasPrefix(k, "config/export/"):
			m := regexpcache.MustCompile(`^(config/export/)(0[0-9]+)(/.*)$`).FindStringSubmatch(k)
			if m != nil {
				move(k, newKey(m), kv.Value)
			} else {
				logger.Infof(`ignored "%s"`, k)
			}
		case strings.HasPrefix(k, "config/mapping/revision/"):
			m := regexpcache.MustCompile(`^(config/mapping/revision/)(0[0-9]+)(/.*)$`).FindStringSubmatch(k)
			if m != nil {
				move(k, newKey(m), kv.Value)
			} else {
				logger.Infof(`ignored "%s"`, k)
			}
		case strings.HasPrefix(k, "secret/export/token/"):
			m := regexpcache.MustCompile(`^(secret/export/token/)(0[0-9]+)(/.*)$`).FindStringSubmatch(k)
			if m != nil {
				move(k, newKey(m), kv.Value)
			} else {
				logger.Infof(`ignored "%s"`, k)
			}
		case strings.HasPrefix(k, "file/"):
			// file/imported/
			m := regexpcache.MustCompile(`^(file/[^/]+/)(0[0-9]+)(/.*)$`).FindStringSubmatch(k)
			if m != nil {
				move(k, newKey(m), kv.Value)
			} else {
				logger.Infof(`ignored "%s"`, k)
			}
		case strings.HasPrefix(k, "slice/"):
			// slice/active/opened/closing/
			m := regexpcache.MustCompile(`^(slice/[^/]+/[^/]+/[^/]+/)(0[0-9]+)(/.*)$`).FindStringSubmatch(k)
			if m != nil {
				move(k, newKey(m), kv.Value)
			} else {
				logger.Infof(`ignored "%s"`, k)
			}
		case strings.HasPrefix(k, "record/"):
			m := regexpcache.MustCompile(`^(record/)(0[0-9]+)(/.*)$`).FindStringSubmatch(k)
			if m != nil {
				move(k, newKey(m), kv.Value)
			} else {
				logger.Infof(`ignored "%s"`, k)
			}
		case strings.HasPrefix(k, "task/"):
			m := regexpcache.MustCompile(`^(task/)(0[0-9]+)(/.*)$`).FindStringSubmatch(k)
			if m != nil {
				move(k, newKey(m), kv.Value)
			} else {
				logger.Infof(`ignored "%s"`, k)
			}
		case strings.HasPrefix(k, "stats/received/"):
			m := regexpcache.MustCompile(`^(stats/received)(0[0-9]+)(/.*)$`).FindStringSubmatch(k)
			if m != nil {
				move(k, newKey(m), kv.Value)
			} else {
				logger.Infof(`ignored "%s"`, k)
			}
		default:
			logger.Infof(`ignoring key "%s"`, k)
		}

		return nil
	}

	err := etcdop.Prefix("").
		GetAll().
		ForEachOp(onKey).
		DoOrErr(context.Background(), client)
	if err != nil {
		return err
	}

	if err := flush(); err != nil {
		return err
	}

	logger.Info("migration done!")
	return markMigrationDone(sess)
}

func waitForMigration(logger log.Logger, client *etcd.Client) error {
	logger.Info(`waiting for migration`)
	for {
		ok, err := migrationDoneKey.Exists().Do(context.Background(), client)
		if err != nil {
			return err
		}
		if ok {
			logger.Info(`migration done`)
			return nil
		}
		<-time.After(time.Second)
	}
}

func markMigrationDone(sess *concurrency.Session) error {
	return migrationDoneKey.Put("done", etcd.WithLease(sess.Lease())).DoOrErr(context.Background(), sess.Client())
}
