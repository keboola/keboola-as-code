// Package task provides a task abstraction for long-running operations in the Worker node.
// It is guaranteed that the task will run at most once, as well as resistance to outages.
package task

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	gonanoid "github.com/matoous/go-nanoid/v2"
	etcd "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/atomic"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdclient"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Node struct {
	clock   clock.Clock
	logger  log.Logger
	schema  *schema.Schema
	client  *etcd.Client
	session *concurrency.Session
	nodeID  string
	config  config
	// wg waits for tasks on shutdown
	wg *sync.WaitGroup
	// tasksCount contains number of running tasks, for logs
	tasksCount *atomic.Int64
}

type Operation func(ctx context.Context, logger log.Logger) (string, error)

type dependencies interface {
	Clock() clock.Clock
	Logger() log.Logger
	Process() *servicectx.Process
	Schema() *schema.Schema
	EtcdClient() *etcd.Client
}

func NewNode(d dependencies, opts ...Option) (*Node, error) {
	// Apply options
	c := defaultConfig()
	for _, o := range opts {
		o(&c)
	}

	proc := d.Process()

	m := &Node{
		clock:      d.Clock(),
		logger:     d.Logger().AddPrefix("[tasks]"),
		schema:     d.Schema(),
		client:     d.EtcdClient(),
		nodeID:     proc.UniqueID(),
		config:     c,
		wg:         &sync.WaitGroup{},
		tasksCount: atomic.NewInt64(0),
	}

	// Create etcd session
	var err error
	m.session, err = etcdclient.CreateConcurrencySession(m.logger, proc, m.client, c.ttlSeconds)
	if err != nil {
		return nil, err
	}

	// Graceful shutdown
	proc.OnShutdown(func() {
		m.logger.Info("received shutdown request")
		if c := m.tasksCount.Load(); c > 0 {
			m.logger.Infof(`waiting for "%d" tasks to be finished`, c)
		}
		m.wg.Wait()
		m.logger.Info("shutdown done")
	})

	return m, nil
}

// StartTask backed by the lock, so the task run at most once.
// The context will be passed to the operation callback and must contain timeout/deadline.
func (n *Node) StartTask(ctx context.Context, exportKey key.ExportKey, lock string, operation Operation) error {
	// Check if a timeout is defined
	_, found := ctx.Deadline()
	if !found {
		return errors.New("task must have a timeout specified via the context")
	}

	taskKey := key.TaskKey{ExportKey: exportKey, CreatedAt: key.UTCTime(n.clock.Now()), RandomSuffix: gonanoid.Must(5)}
	task := model.Task{TaskKey: taskKey, WorkerNode: n.nodeID, Lock: lock}

	taskEtcdKey := n.schema.Tasks().ByKey(task.TaskKey)
	lockEtcdKey := n.schema.Runtime().Lock().Task().InExport(exportKey).LockKey(task.Lock)

	logger := n.logger.AddPrefix(fmt.Sprintf("[taskId=%s]", taskKey.ID()))
	logger.Infof(`new task, key "%s"`, taskKey.String())
	logger.Infof(`acquiring lock "%s"`, task.Lock)

	// Create task and lock in etcd
	// Atomicity: If the lock key already exists, the then the transaction fails and task is ignored.
	// Resistance to outages: If the Worker node fails, the lock is released automatically by the lease, after the session TTL seconds.
	createTaskOp := op.MergeToTxn(
		taskEtcdKey.Put(task),
		lockEtcdKey.PutIfNotExists(task.WorkerNode, etcd.WithLease(n.session.Lease())),
	)
	if resp, err := createTaskOp.Do(ctx, n.client); err != nil {
		return errors.Errorf(`cannot create task: %s`, err)
	} else if !resp.Succeeded {
		logger.Infof(`task ignored, the lock "%s" is in use`, task.Lock)
		return nil
	}
	logger.Infof(`lock "%s" acquired`, task.Lock)

	// Run operation in the background
	n.wg.Add(1)
	n.tasksCount.Inc()
	go func() {
		defer n.wg.Done()
		defer n.tasksCount.Dec()

		// Process results, in defer, to catch panic
		var result string
		var err error
		startTime := n.clock.Now()
		defer func() {
			// Catch panic
			if panicErr := recover(); panicErr != nil {
				result = ""
				err = errors.Errorf("panic: %s", panicErr)
			}

			// Calculate duration
			endTime := n.clock.Now()
			finishedAt := key.UTCTime(endTime)
			duration := endTime.Sub(startTime)

			// Update fields
			task.FinishedAt = &finishedAt
			task.Duration = &duration
			if err == nil {
				task.Result = result
				logger.Infof(`task succeeded (%s): %s`, duration, result)
			} else {
				task.Error = err.Error()
				logger.Warnf(`task failed (%s): %s`, duration, err)
			}

			// If release of the lock takes longer than the ttl, lease is expired anyway
			opCtx, cancel := context.WithTimeout(context.Background(), time.Duration(n.config.ttlSeconds)*time.Second)
			defer cancel()

			// Update task and release lock in etcd
			finishTaskOp := op.MergeToTxn(
				taskEtcdKey.Put(task),
				lockEtcdKey.DeleteIfExists(),
			)
			logger.Infof(`releasing lock "%s"`, task.Lock)
			if resp, err := finishTaskOp.Do(opCtx, n.client); err != nil {
				logger.Errorf(`cannot update task and release lock: %s`, err)
				return
			} else if !resp.Succeeded {
				logger.Errorf(`cannot release task lock "%s", not found`, task.Lock)
				return
			}
		}()

		// Do operation
		result, err = operation(ctx, logger)
	}()
	return nil
}
