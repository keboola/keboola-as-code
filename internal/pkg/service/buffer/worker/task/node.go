// Package task provides a task abstraction for long-running operations in the Worker node.
// It is guaranteed that the task will run at most once, as well as resistance to outages.
package task

import (
	"context"
	"fmt"
	"runtime/debug"
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
	ctx              context.Context
	wg               *sync.WaitGroup
	clock            clock.Clock
	logger           log.Logger
	schema           *schema.Schema
	client           *etcd.Client
	session          *concurrency.Session
	nodeID           string
	config           config
	tasksCount       *atomic.Int64
	runningTasksLock *sync.Mutex
	runningTasks     map[key.TaskKey]bool
}

type Result = string

type Task func(ctx context.Context, logger log.Logger) (Result, error)

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

	n := &Node{
		clock:            d.Clock(),
		logger:           d.Logger().AddPrefix("[task]"),
		schema:           d.Schema(),
		client:           d.EtcdClient(),
		nodeID:           proc.UniqueID(),
		config:           c,
		tasksCount:       atomic.NewInt64(0),
		runningTasksLock: &sync.Mutex{},
		runningTasks:     make(map[key.TaskKey]bool),
	}

	// Create etcd session
	var err error
	n.session, err = etcdclient.CreateConcurrencySession(n.logger, proc, n.client, c.ttlSeconds)
	if err != nil {
		return nil, err
	}

	// Graceful shutdown
	var cancel context.CancelFunc
	n.wg = &sync.WaitGroup{}
	n.ctx, cancel = context.WithCancel(context.Background())
	proc.OnShutdown(func() {
		n.logger.Info("received shutdown request")
		if c := n.tasksCount.Load(); c > 0 {
			n.logger.Infof(`waiting for "%d" tasks to be finished`, c)
		}
		cancel()
		n.wg.Wait()
		n.logger.Info("shutdown done")
	})

	return n, nil
}

func (n *Node) TasksCount() int64 {
	return n.tasksCount.Load()
}

// StartTask backed by the lock, so the task run at most once.
// The context will be passed to the operation callback and must contain timeout/deadline.
func (n *Node) StartTask(ctx context.Context, exportKey key.ExportKey, typ, lock string, operation Task) (*model.Task, error) {
	taskKey := key.TaskKey{ExportKey: exportKey, Type: typ, CreatedAt: key.UTCTime(n.clock.Now()), RandomSuffix: gonanoid.Must(5)}

	// Lock task locally for periodical re-syncs,
	// so locally can be determined that the task is already running.
	ok, unlock := n.lockTask(taskKey)
	if !ok {
		return nil, nil
	}

	// Create task model
	task := model.Task{TaskKey: taskKey, WorkerNode: n.nodeID, Lock: lock}

	// Create task and lock in etcd
	// Atomicity: If the lock key already exists, the then the transaction fails and task is ignored.
	// Resistance to outages: If the Worker node fails, the lock is released automatically by the lease, after the session TTL seconds.
	taskEtcdKey := n.schema.Tasks().ByKey(task.TaskKey)
	lockEtcdKey := n.schema.Runtime().Lock().Task().InExport(exportKey).LockKey(task.Lock)
	logger := n.logger.AddPrefix(fmt.Sprintf("[%s]", taskKey.ID()))
	createTaskOp := op.MergeToTxn(
		taskEtcdKey.Put(task),
		lockEtcdKey.PutIfNotExists(task.WorkerNode, etcd.WithLease(n.session.Lease())),
	)
	if resp, err := createTaskOp.Do(n.ctx, n.client); err != nil {
		unlock()
		return nil, errors.Errorf(`cannot start task "%s": %s`, taskKey, err)
	} else if !resp.Succeeded {
		unlock()
		logger.Infof(`task ignored, the lock "%s" is in use`, lockEtcdKey.Key())
		return nil, nil
	}
	logger.Infof(`started task "%s"`, taskKey)
	logger.Debugf(`lock acquired "%s"`, lockEtcdKey.Key())

	// Run operation in the background
	go func() {
		defer unlock()

		// Process results, in defer, to catch panic
		var result string
		var err error
		startTime := n.clock.Now()
		defer func() {
			// Catch panic
			if panicErr := recover(); panicErr != nil {
				result = ""
				err = errors.Errorf("panic: %s, stacktrace: %s", panicErr, string(debug.Stack()))
				logger.Errorf(`task panic: %s`, err)
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
				logger.Warnf(`task failed (%s): %s`, duration, errors.Format(err, errors.FormatWithStack()))
			}

			// If release of the lock takes longer than the ttl, lease is expired anyway
			opCtx, cancel := context.WithTimeout(context.Background(), time.Duration(n.config.ttlSeconds)*time.Second)
			defer cancel()

			// Update task and release lock in etcd
			finishTaskOp := op.MergeToTxn(
				taskEtcdKey.Put(task),
				lockEtcdKey.DeleteIfExists(),
			)
			if resp, err := finishTaskOp.Do(opCtx, n.client); err != nil {
				logger.Errorf(`cannot update task and release lock: %s`, err)
				return
			} else if !resp.Succeeded {
				logger.Errorf(`cannot release task lock "%s", not found`, lockEtcdKey.Key())
				return
			}
			logger.Debugf(`lock released "%s"`, lockEtcdKey.Key())
		}()

		// Do operation
		result, err = operation(ctx, logger)
	}()
	return &task, nil
}

func (n *Node) lockTask(taskKey key.TaskKey) (ok bool, unlock func()) {
	n.runningTasksLock.Lock()
	defer n.runningTasksLock.Unlock()
	if n.runningTasks[taskKey] {
		return false, nil
	}

	n.wg.Add(1)
	n.tasksCount.Inc()
	n.runningTasks[taskKey] = true

	return true, func() {
		n.runningTasksLock.Lock()
		defer n.runningTasksLock.Unlock()
		delete(n.runningTasks, taskKey)
		n.tasksCount.Dec()
		n.wg.Done()
	}
}
