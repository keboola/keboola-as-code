package task

import (
	"context"
	"runtime/debug"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	etcd "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/atomic"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
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
	spanName       = "keboola.go.task"
)

// Fn represents a task operation.
type Fn func(ctx context.Context, logger log.Logger) Result

type runTaskFn func() (Result, error)

// Node represents a cluster Worker node on which tasks are run.
// See comments in the StartTask method.
type Node struct {
	tracer telemetry.Tracer
	meters *meters

	clock  clock.Clock
	logger log.Logger
	client *etcd.Client

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

func NewNode(ctx context.Context, d dependencies, opts ...NodeOption) (*Node, error) {
	// Apply options
	c := defaultNodeConfig()
	for _, o := range opts {
		o(&c)
	}

	proc := d.Process()

	taskPrefix := etcdop.NewTypedPrefix[Task](etcdop.NewPrefix(c.taskEtcdPrefix), d.EtcdSerde())
	n := &Node{
		tracer:         d.Telemetry().Tracer(),
		meters:         newMeters(d.Telemetry().Meter()),
		clock:          d.Clock(),
		logger:         d.Logger().WithComponent("task"),
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
	n.tasksCtx, cancelTasks = context.WithCancel(context.Background()) // nolint: contextcheck
	sessionWg := &sync.WaitGroup{}
	sessionCtx, cancelSession := context.WithCancel(ctx)
	proc.OnShutdown(func(ctx context.Context) {
		ctx = ctxattr.ContextWith(ctx, attribute.String("node", n.nodeID))
		n.logger.InfoCtx(ctx, "received shutdown request")
		if c := n.tasksCount.Load(); c > 0 {
			n.logger.InfofCtx(ctx, `waiting for "%d" tasks to be finished`, c)
		}
		cancelTasks()
		n.tasksWg.Wait()
		cancelSession()
		sessionWg.Wait()
		n.logger.InfoCtx(ctx, "shutdown done")
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

// StartTaskOrErr in background, the task run at most once, it is provided by local lock and etcd transaction.
func (n *Node) StartTaskOrErr(ctx context.Context, cfg Config) error {
	_, err := n.StartTask(ctx, cfg)
	return err
}

// StartTask in background, the task run at most once, it is provided by local lock and etcd transaction.
func (n *Node) StartTask(ctx context.Context, cfg Config) (t *Task, err error) {
	// Prepare task, acquire lock
	task, fn, err := n.prepareTask(ctx, cfg)

	// Run task in background, if it is prepared
	if fn != nil {
		go func() {
			// Error is logged and stored to DB
			_, _ = fn()
		}()
	}

	return task, err
}

// RunTaskOrErr in foreground, the task run at most once, it is provided by local lock and etcd transaction.
func (n *Node) RunTaskOrErr(cfg Config) error {
	_, err := n.RunTask(cfg)
	return err
}

// RunTask in foreground, the task run at most once, it is provided by local lock and etcd transaction.
func (n *Node) RunTask(cfg Config) (t *Task, err error) {
	// Prepare task, acquire lock, handle error during prepare phase
	task, fn, err := n.prepareTask(n.tasksCtx, cfg)
	if err != nil {
		return nil, err
	}

	// No-op, for example a task with the same lock is already running
	if fn == nil {
		return nil, nil
	}

	// Run task in foreground, handle error during task execution
	result, err := fn()
	if err != nil {
		return t, err
	}

	// Handle error during task itself
	return task, result.Error
}

func (n *Node) prepareTask(ctx context.Context, cfg Config) (t *Task, fn runTaskFn, err error) {
	if err := cfg.Validate(); err != nil {
		panic(err)
	}

	// Generate lock name if it is not set
	if cfg.Lock == "" {
		cfg.Lock = cfg.Key.String()
	}
	lock := LockEtcdPrefix.Key(cfg.Lock)

	// Append datetime and a random suffix to the task ID
	createdAt := utctime.UTCTime(n.clock.Now())
	taskKey := cfg.Key
	taskKey.TaskID = ID(string(cfg.Key.TaskID) + "/" + createdAt.String() + "_" + idgenerator.Random(5))

	// Lock task locally for periodical re-syncs,
	// so locally can be determined that the task is already running.
	ok, unlock := n.lockTaskLocally(lock.Key())
	if !ok {
		return nil, nil, nil
	}

	// Create task model
	task := Task{Key: taskKey, Type: cfg.Type, CreatedAt: createdAt, Node: n.nodeID, Lock: lock}

	// Get session
	n.sessionLock.RLock()
	session := n.session
	n.sessionLock.RUnlock()

	ctx = ctxattr.ContextWith(ctx, attribute.String("task", task.Key.String()), attribute.String("node", n.nodeID))

	// Create task and lock in etcd
	// Atomicity: If the lock key already exists, the then the transaction fails and task is ignored.
	// Resistance to outages: If the Worker node fails, the lock is released automatically by the lease, after the session TTL seconds.
	createTaskOp := op.MergeToTxn(
		n.taskEtcdPrefix.Key(taskKey.String()).Put(task),
		lock.PutIfNotExists(task.Node, etcd.WithLease(session.Lease())),
	)
	if resp, err := createTaskOp.Do(n.tasksCtx, n.client); err != nil { // nolint: contextcheck
		unlock()
		return nil, nil, errors.Errorf(`cannot start task "%s": %s`, taskKey, err)
	} else if !resp.Succeeded {
		unlock()
		n.logger.InfofCtx(ctx, `task ignored, the lock "%s" is in use`, lock.Key())
		return nil, nil, nil
	}

	// Run operation in the background
	n.logger.InfofCtx(ctx, `started task`)
	n.logger.DebugfCtx(ctx, `lock acquired "%s"`, task.Lock.Key())

	// Return function, task is prepared, lock is locked, it can be run in background/foreground.
	fn = func() (Result, error) {
		defer unlock()
		return n.runTask(n.logger, task, cfg)
	}
	return &task, fn, nil
}

func (n *Node) runTask(logger log.Logger, task Task, cfg Config) (result Result, err error) {
	// Create task context
	ctx, cancel := cfg.Context()
	ctx = ctxattr.ContextWith(ctx, attribute.String("task", task.Key.String()), attribute.String("node", n.nodeID))

	defer cancel()
	if _, ok := ctx.Deadline(); !ok {
		panic(errors.Errorf(`task "%s" context must have a deadline`, cfg.Type))
	}

	// Setup telemetry
	ctx, span := n.tracer.Start(ctx, spanName, trace.WithAttributes(spanStartAttrs(&task)...))
	n.meters.running.Add(ctx, 1, metric.WithAttributes(meterStartAttrs(&task)...))

	// Process results in defer to catch panic
	defer span.End(&result.Error)

	// Do operation
	startTime := n.clock.Now()
	func() {
		defer func() {
			if panicErr := recover(); panicErr != nil {
				err := errors.Errorf("panic: %s, stacktrace: %s", panicErr, string(debug.Stack()))
				logger.ErrorfCtx(ctx, `task panic: %s`, err)
				if result.Error == nil {
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
	durationMs := float64(duration) / float64(time.Millisecond)

	// Update fields
	task.FinishedAt = &finishedAt
	task.Duration = &duration
	task.Outputs = result.Outputs
	if result.Error == nil {
		task.Result = result.Result
		if len(task.Outputs) > 0 {
			logger.InfofCtx(ctx, `task succeeded (%s): %s outputs: %s`, duration, task.Result, json.MustEncodeString(task.Outputs, false))
		} else {
			logger.InfofCtx(ctx, `task succeeded (%s): %s`, duration, task.Result)
		}
	} else {
		task.Error = result.Error.Error()
		if len(task.Outputs) > 0 {
			logger.WarnfCtx(ctx, `task failed (%s): %s outputs: %s`, duration, errors.Format(result.Error, errors.FormatWithStack()), json.MustEncodeString(task.Outputs, false))
		} else {
			logger.WarnfCtx(ctx, `task failed (%s): %s`, duration, errors.Format(result.Error, errors.FormatWithStack()))
		}
	}

	// Create context for task finalization, the original context could have timed out.
	// If release of the lock takes longer than the ttl, lease is expired anyway.
	finalizationCtx, finalizationCancel := context.WithTimeout(context.Background(), time.Duration(n.config.ttlSeconds)*time.Second)
	defer finalizationCancel()

	// Update telemetry
	span.SetAttributes(spanEndAttrs(&task, result)...)
	n.meters.running.Add(finalizationCtx, -1, metric.WithAttributes(meterStartAttrs(&task)...))
	n.meters.duration.Record(finalizationCtx, durationMs, metric.WithAttributes(meterEndAttrs(&task, result)...))

	// Update task and release lock in etcd
	finalizeTaskOp := op.MergeToTxn(
		n.taskEtcdPrefix.Key(task.Key.String()).Put(task),
		task.Lock.DeleteIfExists(),
	)
	if resp, err := finalizeTaskOp.Do(finalizationCtx, n.client); err != nil {
		err = errors.Errorf(`cannot update task and release lock: %w`, err)
		logger.ErrorCtx(ctx, err)
		return result, err
	} else if !resp.Succeeded {
		err = errors.Errorf(`cannot release task lock "%s", not found`, task.Lock.Key())
		logger.ErrorCtx(ctx, err)
		return result, err
	}
	logger.DebugfCtx(ctx, `lock released "%s"`, task.Lock.Key())

	return result, nil
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
