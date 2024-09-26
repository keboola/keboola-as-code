package task

import (
	"context"
	"reflect"
	"runtime/debug"
	"sort"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/spf13/cast"
	etcd "go.etcd.io/etcd/client/v3"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/atomic"

	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
	svcerrors "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
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
	tracer  telemetry.Tracer
	metrics *metrics

	clock  clock.Clock
	logger log.Logger
	client *etcd.Client

	tasksWg *sync.WaitGroup

	session *etcdop.Session

	nodeID     string
	config     nodeConfig
	tasksCount *atomic.Int64

	taskEtcdPrefix etcdop.PrefixT[Task]
	taskLocksMutex *sync.Mutex
	taskLocks      map[string]bool

	exceptionIDPrefix string
}

type dependencies interface {
	Telemetry() telemetry.Telemetry
	Clock() clock.Clock
	Logger() log.Logger
	Process() *servicectx.Process
	EtcdClient() *etcd.Client
	EtcdSerde() *serde.Serde
}

func NewNode(nodeID string, exceptionIDPrefix string, d dependencies, opts ...NodeOption) (*Node, error) {
	// Validate
	if nodeID == "" {
		panic(errors.New("task.Node: node ID cannot be empty"))
	}

	// Apply options
	c := defaultNodeConfig()
	for _, o := range opts {
		o(&c)
	}

	proc := d.Process()

	n := &Node{
		tracer:            d.Telemetry().Tracer(),
		metrics:           newMetrics(d.Telemetry().Meter()),
		clock:             d.Clock(),
		logger:            d.Logger().WithComponent("task"),
		client:            d.EtcdClient(),
		nodeID:            nodeID,
		config:            c,
		tasksCount:        atomic.NewInt64(0),
		taskEtcdPrefix:    newTaskPrefix(d.EtcdSerde()),
		taskLocksMutex:    &sync.Mutex{},
		taskLocks:         make(map[string]bool),
		exceptionIDPrefix: exceptionIDPrefix,
	}

	// Graceful shutdown
	bgContext := ctxattr.ContextWith(context.Background(), attribute.String("node", n.nodeID)) // nolint: contextcheck
	n.tasksWg = &sync.WaitGroup{}
	sessionWg := &sync.WaitGroup{}
	sessionCtx, cancelSession := context.WithCancel(bgContext)
	proc.OnShutdown(func(ctx context.Context) {
		ctx = ctxattr.ContextWith(ctx, attribute.String("node", n.nodeID))
		n.logger.Info(ctx, "received shutdown request")
		if c := n.tasksCount.Load(); c > 0 {
			n.logger.Infof(ctx, `waiting for "%d" tasks to be finished`, c)
		}
		n.tasksWg.Wait()
		cancelSession()
		sessionWg.Wait()
		n.logger.Info(ctx, "shutdown done")
	})

	// Log node ID
	n.logger.Infof(bgContext, `node ID "%s"`, n.nodeID)

	// Create etcd session
	session, errCh := etcdop.
		NewSessionBuilder().
		WithTTLSeconds(c.ttlSeconds).
		Start(sessionCtx, sessionWg, n.logger, n.client)
	if err := <-errCh; err == nil {
		n.session = session
	} else {
		return nil, err
	}

	return n, nil
}

func (n *Node) GetTask(k Key) op.WithResult[Task] {
	return n.taskEtcdPrefix.Key(k.String()).GetOrErr(n.client)
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
func (n *Node) StartTask(ctx context.Context, cfg Config) (t Task, err error) {
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
func (n *Node) RunTaskOrErr(ctx context.Context, cfg Config) error {
	_, err := n.RunTask(ctx, cfg)
	return err
}

// RunTask in foreground, the task run at most once, it is provided by local lock and etcd transaction.
func (n *Node) RunTask(ctx context.Context, cfg Config) (t Task, err error) {
	// Prepare task, acquire lock, handle error during prepare phase
	task, fn, err := n.prepareTask(ctx, cfg)
	if err != nil {
		return Task{}, err
	}

	// No-op, for example a task with the same lock is already running
	if fn == nil {
		return Task{}, nil
	}

	// Run task in foreground, handle error during task execution
	result, err := fn()
	if err != nil {
		return t, err
	}

	// Handle error during task itself
	return task, result.Error
}

func (n *Node) prepareTask(ctx context.Context, cfg Config) (t Task, fn runTaskFn, err error) {
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
		return Task{}, nil, TaskLockError{errors.Errorf("task already running %q", taskKey)}
	}

	// Create task model
	task := Task{Key: taskKey, Type: cfg.Type, CreatedAt: createdAt, Node: n.nodeID, Lock: lock}

	// Get session
	session, err := n.session.Session()
	if err != nil {
		return Task{}, nil, err
	}

	ctx = ctxattr.ContextWith(ctx, attribute.String("task", task.Key.String()), attribute.String("node", n.nodeID))

	// Create task and lock in etcd
	// Atomicity: If the lock key already exists, the then the transaction fails and task is ignored.
	// Resistance to outages: If the Worker node fails, the lock is released automatically by the lease, after the session TTL seconds.
	createTaskOp := op.MergeToTxn(
		n.client,
		n.taskEtcdPrefix.Key(taskKey.String()).Put(n.client, task),
		lock.PutIfNotExists(n.client, task.Node, etcd.WithLease(session.Lease())),
	)
	if r := createTaskOp.Do(ctx); r.Err() != nil { // nolint: contextcheck
		unlock()
		return Task{}, nil, errors.Errorf(`cannot start task "%s": %s`, taskKey, r.Err())
	} else if !r.Succeeded() {
		unlock()
		n.logger.Infof(ctx, `task ignored, the lock "%s" is in use`, lock.Key())
		return Task{}, nil, nil
	}

	// Run operation in the background
	n.logger.Infof(ctx, `started task`)
	n.logger.Debugf(ctx, `lock acquired "%s"`, task.Lock.Key())

	// Return function, task is prepared, lock is locked, it can be run in background/foreground.
	fn = func() (Result, error) {
		defer unlock()
		return n.runTask(n.logger, task, cfg)
	}
	return task, fn, nil
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
	n.metrics.running.Add(ctx, 1, metric.WithAttributes(meterStartAttrs(&task)...))

	// Process results in defer to catch panic
	defer span.End(&result.Error)

	// Do operation
	startTime := n.clock.Now()
	func() {
		defer func() {
			if panicErr := recover(); panicErr != nil {
				err := errors.Errorf("panic: %s, stacktrace: %s", panicErr, string(debug.Stack()))
				logger.Errorf(ctx, `task panic: %s`, err)
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

	// Use task outputs in log message and telemetry
	var attrs []attribute.KeyValue
	for k, v := range task.Outputs {
		// Skip nil values
		if v := reflect.ValueOf(v); !v.IsValid() || (v.Kind() == reflect.Pointer && v.IsZero()) {
			continue
		}
		// Convert value to a string if possible
		if str, err := cast.ToStringE(v); err == nil {
			attrs = append(attrs, attribute.String("result_outputs."+k, str))
		}
	}
	sort.SliceStable(attrs, func(i, j int) bool {
		return attrs[i].Key < attrs[j].Key
	})

	// Set task entity result
	if result.Error == nil {
		task.Result = result.Result
	} else {
		task.Error = result.Error.Error()
		task.UserError = &Error{}

		var errWithName svcerrors.WithName
		if errors.As(result.Error, &errWithName) {
			task.UserError.Name = errWithName.ErrorName()
		} else {
			task.UserError.Name = "unknownError"
		}

		var errWithUserMessage svcerrors.WithUserMessage
		if errors.As(result.Error, &errWithUserMessage) {
			task.UserError.Message = errWithUserMessage.ErrorUserMessage()
		} else {
			task.UserError.Message = "Unknown error"
		}

		var errWithExceptionID svcerrors.WithExceptionID
		if errors.As(result.Error, &errWithExceptionID) {
			task.UserError.ExceptionID = errWithExceptionID.ErrorExceptionID()
		} else {
			task.UserError.ExceptionID = n.exceptionIDPrefix + idgenerator.TaskExceptionID()
		}
	}

	// Update telemetry
	span.SetAttributes(spanEndAttrs(&task, result)...)
	span.SetAttributes(attrs...)

	// Log task result
	logger = logger.With(attrs...)
	if result.Error == nil {
		logger.Infof(ctx, `task succeeded (%s): %s`, duration, task.Result)
	} else {
		logger.Warnf(ctx, `task failed (%s): %s`, duration, errors.Format(result.Error, errors.FormatWithStack()))
	}

	// Create context for task finalization, the original context could have timed out.
	// If release of the lock takes longer than the ttl, lease is expired anyway.
	finalizationCtx, finalizationCancel := context.WithTimeout(context.Background(), time.Duration(n.config.ttlSeconds)*time.Second)
	defer finalizationCancel()

	// Update telemetry
	n.metrics.running.Add(finalizationCtx, -1, metric.WithAttributes(meterStartAttrs(&task)...))
	n.metrics.duration.Record(finalizationCtx, durationMs, metric.WithAttributes(meterEndAttrs(&task, result)...))

	// Update task and release lock in etcd
	finalizeTaskOp := op.MergeToTxn(
		n.client,
		n.taskEtcdPrefix.Key(task.Key.String()).Put(n.client, task),
		task.Lock.DeleteIfExists(n.client),
	)
	r := finalizeTaskOp.Do(finalizationCtx)
	if err := r.Err(); err != nil {
		err = errors.Errorf(`cannot update task and release lock: %w`, err)
		logger.Error(ctx, err.Error())
		return result, err
	} else if !r.Succeeded() {
		err = errors.Errorf(`cannot release task lock "%s", not found`, task.Lock.Key())
		logger.Error(ctx, err.Error())
		return result, err
	}
	logger.Debugf(ctx, `lock released "%s"`, task.Lock.Key())

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

func newTaskPrefix(s *serde.Serde) etcdop.PrefixT[Task] {
	return etcdop.NewTypedPrefix[Task](etcdop.NewPrefix(TaskEtcdPrefix), s)
}
