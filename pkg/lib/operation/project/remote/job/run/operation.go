package run

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/keboola/go-client/pkg/keboola"
	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type dependencies interface {
	KeboolaProjectAPI() *keboola.API
	ProjectFeatures() keboola.FeaturesMap
	Logger() log.Logger
	Tracer() trace.Tracer
}

type RunOptions struct {
	Jobs    []Job
	Async   bool
	Timeout time.Duration
}

func Run(ctx context.Context, o RunOptions, d dependencies) (err error) {
	ctx, span := d.Tracer().Start(ctx, "kac.lib.operation.project.remote.job.run")
	defer telemetry.EndSpan(span, &err)

	logger := d.Logger()
	api := d.KeboolaProjectAPI()
	timeoutCtx, cancel := context.WithTimeout(ctx, o.Timeout)
	defer cancel()

	queue := &JobQueue{
		async:          o.Async,
		hasQueueV2:     d.ProjectFeatures().Has("queuev2"),
		ctx:            timeoutCtx,
		api:            api,
		wg:             &sync.WaitGroup{},
		err:            errors.NewMultiError(),
		remaining:      map[string]bool{},
		remainingMutex: &sync.Mutex{},
		done:           make(chan struct{}),
	}

	if len(o.Jobs) > 1 {
		logger.Infof("Starting %d jobs.", len(o.Jobs))
	} else {
		logger.Info("Starting job.")
	}

	// dispatch jobs in parallel
	for _, job := range o.Jobs {
		job := job
		queue.dispatch(&job)
	}

	queue.startLogRemaining(logger)
	err = queue.wait()
	queue.stopLogRemaining()

	if err != nil {
		logger.Error("Some jobs failed, see below.")
		return err
	} else {
		if !o.Async {
			logger.Info("Finished running all jobs.")
		} else {
			logger.Info("Started all jobs.")
		}
		return nil
	}
}

type JobQueue struct {
	async      bool
	hasQueueV2 bool

	ctx context.Context
	api *keboola.API

	wg  *sync.WaitGroup
	err errors.MultiError

	remaining      map[string]bool
	remainingMutex *sync.Mutex

	done chan struct{}
}

func (q *JobQueue) startLogRemaining(logger log.Logger) {
	go func() {
		for {
			select {
			case <-q.ctx.Done():
				break
			case <-q.done:
				break
			case <-time.After(time.Second * 5):
				logger.Infof("Waiting for %s", strings.Join(q.getRemainingJobs(), ", "))
			}
		}
	}()
}

func (q *JobQueue) stopLogRemaining() {
	q.done <- struct{}{}
}

func (q *JobQueue) getRemainingJobs() []string {
	q.remainingMutex.Lock()
	remainingJobs := []string{}
	for k, v := range q.remaining {
		if v {
			remainingJobs = append(remainingJobs, k)
		}
	}
	q.remainingMutex.Unlock()
	return remainingJobs
}

func (q *JobQueue) dispatch(job *Job) {
	q.remaining[job.Key] = true
	q.wg.Add(1)

	go func() {
		defer q.wg.Done()

		if e := job.Run(q.ctx, q.api, q.async, q.hasQueueV2); e != nil {
			q.err.Append(e)
		}

		q.remainingMutex.Lock()
		q.remaining[job.Key] = false
		q.remainingMutex.Unlock()
	}()
}

func (q *JobQueue) wait() error {
	q.wg.Wait()
	return q.err.ErrorOrNil()
}

type Job struct {
	Key         string
	BranchID    keboola.BranchID
	ComponentID keboola.ComponentID
	ConfigID    keboola.ConfigID
}

func (o *Job) Run(ctx context.Context, api *keboola.API, async bool, hasQueueV2 bool) error {
	if hasQueueV2 {
		job, err := api.CreateQueueJobRequest(o.ComponentID, o.ConfigID).Send(ctx)
		if err != nil {
			return err
		}

		if !async {
			err = api.WaitForQueueJob(ctx, job)
			if err != nil {
				return err
			}
		}
	} else {
		// nolint: staticcheck
		job, err := api.CreateOldQueueJobRequest(o.ComponentID, o.ConfigID, keboola.WithBranchID(o.BranchID)).Send(ctx)
		if err != nil {
			return err
		}

		if !async {
			// nolint: staticcheck
			err = api.WaitForOldQueueJob(ctx, job.ID)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
