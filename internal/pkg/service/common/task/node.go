package task

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	etcd "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/atomic"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	LockEtcdPrefix = etcdop.Prefix("runtime/lock/task")
)

type Fn func(ctx context.Context, logger log.Logger) Result

// Node represents a cluster Worker node on which tasks are run.
// See comments in the StartTask method.
type Node struct {
	tracer   telemetry.Tracer
	clock    clock.Clock
	logger   log.Logger
	client   *etcd.Client
	tasksCtx context.Context
	tasksWg  *sync.WaitGroup

	sessionLock *sync.RWMutex
	session     *concurrency.Session

	nodeID     string
	config     nodeConfig
	tasksCount *atomic.Int64

	taskEtcdPrefix etcdop.PrefixT[Task]
	taskLocksMutex *sync.Mutex
	taskLocks      map[string]bool
}

type dependencies interface {
	Telemetry() telemetry.Telemetry
	Clock() clock.Clock
	Logger() log.Logger
	Process() *servicectx.Process
	EtcdClient() *etcd.Client
	EtcdSerde() *serde.Serde
}

func NewNode(d dependencies, opts ...NodeOption) (*Node, error) {
	// Apply options
	c := defaultNodeConfig()
	for _, o := range opts {
		o(&c)
	}

	proc := d.Process()

	taskPrefix := etcdop.NewTypedPrefix[Task](etcdop.NewPrefix(c.taskEtcdPrefix), d.EtcdSerde())
	n := &Node{
		tracer:         d.Telemetry().Tracer(),
		clock:          d.Clock(),
		logger:         d.Logger().AddPrefix("[task]"),
		client:         d.EtcdClient(),
		nodeID:         proc.UniqueID(),
		config:         c,
		tasksCount:     atomic.NewInt64(0),
		taskEtcdPrefix: taskPrefix,
		taskLocksMutex: &sync.Mutex{},
		taskLocks:      make(map[string]bool),
	}

	// Graceful shutdown
	var cancelTasks context.CancelFunc
	n.tasksWg = &sync.WaitGroup{}
	n.tasksCtx, cancelTasks = context.WithCancel(context.Background())
	sessionWg := &sync.WaitGroup{}
	sessionCtx, cancelSession := context.WithCancel(context.Background())
	proc.OnShutdown(func() {
		n.logger.Info("received shutdown request")
		if c := n.tasksCount.Load(); c > 0 {
			n.logger.Infof(`waiting for "%d" tasks to be finished`, c)
		}
		cancelTasks()
		n.tasksWg.Wait()
		cancelSession()
		sessionWg.Wait()
		n.logger.Info("shutdown done")
	})

	// Create etcd session
	n.sessionLock = &sync.RWMutex{}
	sessionInit := etcdop.ResistantSession(sessionCtx, sessionWg, n.logger, n.client, c.ttlSeconds, func(session *concurrency.Session) error {
		n.sessionLock.Lock()
		n.session = session
		n.sessionLock.Unlock()
		return nil
	})

	if err := <-sessionInit; err != nil {
		return nil, err
	}

	return n, nil
}

func (n *Node) TasksCount() int64 {
	return n.tasksCount.Load()
}

func (n *Node) StartTaskOrErr(cfg Config) error {
	_, err := n.StartTask(cfg)
	return err
}

// StartTask backed by local lock and etcd transaction, so the task run at most once.
// The context will be passed to the operation callback.
func (n *Node) StartTask(cfg Config) (t *Task, err error) {
	if err := cfg.Validate(); err != nil {
		panic(err)
	}

	// Generate lock name if it is not set
	if cfg.Lock == "" {
		cfg.Lock = cfg.Key.String()
	}

	// Lock etcd key
	lock := LockEtcdPrefix.Key(cfg.Lock)

	// Append datetime and a random suffix to the task ID
	createdAt := utctime.UTCTime(n.clock.Now())
	taskKey := cfg.Key
	taskKey.TaskID = ID(string(cfg.Key.TaskID) + "/" + createdAt.String() + "_" + idgenerator.Random(5))

	// Lock task locally for periodical re-syncs,
	// so locally can be determined that the task is already running.
	ok, unlock := n.lockTaskLocally(lock.Key())
	if !ok {
		return nil, nil
	}

	// Create task model
	task := Task{Key: taskKey, Type: cfg.Type, CreatedAt: createdAt, Node: n.nodeID, Lock: lock}

	// Get session
	n.sessionLock.RLock()
	session := n.session
	n.sessionLock.RUnlock()

	// Create task and lock in etcd
	// Atomicity: If the lock key already exists, the then the transaction fails and task is ignored.
	// Resistance to outages: If the Worker node fails, the lock is released automatically by the lease, after the session TTL seconds.
	logger := n.logger.AddPrefix(fmt.Sprintf("[%s]", taskKey.String()))
	createTaskOp := op.MergeToTxn(
		n.taskEtcdPrefix.Key(taskKey.String()).Put(task),
		lock.PutIfNotExists(task.Node, etcd.WithLease(session.Lease())),
	)
	if resp, err := createTaskOp.Do(n.tasksCtx, n.client); err != nil {
		unlock()
		return nil, errors.Errorf(`cannot start task "%s": %s`, taskKey, err)
	} else if !resp.Succeeded {
		unlock()
		logger.Infof(`task ignored, the lock "%s" is in use`, lock.Key())
		return nil, nil
	}

	// Run operation in the background
	logger.Infof(`started task`)
	logger.Debugf(`lock acquired "%s"`, task.Lock.Key())
	go func() {
		defer unlock()
		n.runTask(logger, task, cfg)
	}()

	return &task, nil
}

func (n *Node) runTask(logger log.Logger, task Task, cfg Config) {
	// Create task context
	ctx, cancel := cfg.Context()
	defer cancel()
	if _, ok := ctx.Deadline(); !ok {
		panic(errors.Errorf(`task "%s" context must have a deadline`, cfg.Type))
	}

	// Setup telemetry
	ctx, span := n.tracer.Start(ctx, n.config.spanNamePrefix+"."+cfg.Type, trace.WithAttributes(
		attribute.String("projectId", task.ProjectID.String()),
		attribute.String("taskId", task.TaskID.String()),
		attribute.String("taskType", cfg.Type),
		attribute.String("lock", task.Lock.Key()),
		attribute.String("node", task.Node),
		attribute.String("createdAt", task.CreatedAt.String()),
	))

	// Process results in defer to catch panic
	var result Result
	defer span.End(&result.error)

	// Do operation
	startTime := n.clock.Now()
	func() {
		defer func() {
			if panicErr := recover(); panicErr != nil {
				err := errors.Errorf("panic: %s, stacktrace: %s", panicErr, string(debug.Stack()))
				logger.Errorf(`task panic: %s`, err)
				if result.error == nil {
					result = ErrResult(err)
				}
			}
		}()
		result = cfg.Operation(ctx, logger)
	}()

	// Calculate duration
	endTime := n.clock.Now()
	finishedAt := utctime.UTCTime(endTime)
	duration := endTime.Sub(startTime)

	// Update fields
	task.FinishedAt = &finishedAt
	task.Duration = &duration
	if result.error == nil {
		task.Result = result.result
		task.Outputs = result.outputs
		if len(task.Outputs) > 0 {
			logger.Infof(`task succeeded (%s): %s outputs: %s`, duration, task.Result, json.MustEncodeString(task.Outputs, false))
		} else {
			logger.Infof(`task succeeded (%s): %s`, duration, task.Result)
		}
	} else {
		task.Error = result.error.Error()
		task.Outputs = result.outputs
		if len(task.Outputs) > 0 {
			logger.Warnf(`task failed (%s): %s outputs: %s`, duration, errors.Format(result.error, errors.FormatWithStack()), json.MustEncodeString(task.Outputs, false))
		} else {
			logger.Warnf(`task failed (%s): %s`, duration, errors.Format(result.error, errors.FormatWithStack()))
		}
	}
	span.SetAttributes(
		attribute.Float64("duration", task.Duration.Seconds()),
		attribute.String("result", task.Result),
		attribute.String("finishedAt", task.FinishedAt.String()),
	)

	// If release of the lock takes longer than the ttl, lease is expired anyway
	opCtx, opCancel := context.WithTimeout(context.Background(), time.Duration(n.config.ttlSeconds)*time.Second)
	defer opCancel()

	// Update task and release lock in etcd
	finishTaskOp := op.MergeToTxn(
		n.taskEtcdPrefix.Key(task.Key.String()).Put(task),
		task.Lock.DeleteIfExists(),
	)
	if resp, err := finishTaskOp.Do(opCtx, n.client); err != nil {
		logger.Errorf(`cannot update task and release lock: %s`, err)
		return
	} else if !resp.Succeeded {
		logger.Errorf(`cannot release task lock "%s", not found`, task.Lock.Key())
		return
	}
	logger.Debugf(`lock released "%s"`, task.Lock.Key())
}

// lockTaskLocally guarantees that the task runs at most once on the Worker node.
// Uniqueness within the cluster is guaranteed by the etcd transaction, see StartTask method.
func (n *Node) lockTaskLocally(lock string) (ok bool, unlock func()) {
	n.taskLocksMutex.Lock()
	defer n.taskLocksMutex.Unlock()
	if n.taskLocks[lock] {
		return false, nil
	}

	n.tasksWg.Add(1)
	n.tasksCount.Inc()
	n.taskLocks[lock] = true

	return true, func() {
		n.taskLocksMutex.Lock()
		defer n.taskLocksMutex.Unlock()
		delete(n.taskLocks, lock)
		n.tasksCount.Dec()
		n.tasksWg.Done()
	}
}
