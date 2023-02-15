package run

import (
	"context"
	"fmt"
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
	Jobs    []*Job
	Async   bool
	Timeout time.Duration
}

func Run(ctx context.Context, o RunOptions, d dependencies) (err error) {
	ctx, span := d.Tracer().Start(ctx, "kac.lib.operation.project.remote.job.run")
	defer telemetry.EndSpan(span, &err)

	timeoutCtx, cancel := context.WithTimeout(ctx, o.Timeout)
	defer cancel()

	queue := &JobQueue{
		async:          o.Async,
		hasQueueV2:     d.ProjectFeatures().Has("queuev2"),
		ctx:            timeoutCtx,
		api:            d.KeboolaProjectAPI(),
		logger:         d.Logger(),
		wg:             &sync.WaitGroup{},
		err:            errors.NewMultiError(),
		remaining:      map[string]bool{},
		remainingMutex: &sync.Mutex{},
		done:           make(chan struct{}),
	}

	if len(o.Jobs) > 1 {
		queue.logger.Infof("Starting %d jobs.", len(o.Jobs))
	} else {
		queue.logger.Info("Starting job.")
	}

	// dispatch jobs in parallel
	for _, job := range o.Jobs {
		queue.dispatch(job)
	}

	queue.startLogRemaining()
	err = queue.wait()
	queue.stopLogRemaining()

	if err != nil {
		queue.logger.Error("Some jobs failed, see below.")
		return err
	} else {
		if !o.Async {
			queue.logger.Info("Finished running all jobs.")
		} else {
			queue.logger.Info("Started all jobs.")
		}
		return nil
	}
}

type JobQueue struct {
	async      bool
	hasQueueV2 bool

	ctx    context.Context
	api    *keboola.API
	logger log.Logger

	wg  *sync.WaitGroup
	err errors.MultiError

	remaining      map[string]bool
	remainingMutex *sync.Mutex

	done chan struct{}
}

func (q *JobQueue) startLogRemaining() {
	go func() {
		for {
			select {
			case <-q.ctx.Done():
				break
			case <-q.done:
				break
			case <-time.After(time.Second * 5):
				q.logger.Infof("Waiting for %s", strings.Join(q.getRemainingJobs(), ", "))
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

func (q *JobQueue) started(job *Job) {
	q.logger.Infof("Started job \"%s\" using config \"%s\"", job.id, job.Key())

	q.remainingMutex.Lock()
	q.remaining[string(job.id)] = true
	q.remainingMutex.Unlock()
}

func (q *JobQueue) finished(job *Job) {
	q.remainingMutex.Lock()
	q.remaining[string(job.id)] = false
	q.remainingMutex.Unlock()

	q.logger.Infof("Finished job \"%s\"", job.id)
}

func (q *JobQueue) dispatch(job *Job) {
	q.wg.Add(1)

	go func() {
		defer q.wg.Done()

		if err := job.Start(q.ctx, q.api, q.async, q.hasQueueV2); err != nil {
			q.err.Append(errors.Errorf("job \"%s\" failed to start: %s", job.Key(), err))
			return
		}

		if !q.async {
			q.started(job)
			if err := job.Wait(); err != nil {
				q.err.Append(errors.Errorf("job \"%s\" failed: %s", job.Key(), err))
			}
			q.finished(job)
		}
	}()
}

func (q *JobQueue) wait() error {
	q.wg.Wait()
	return q.err.ErrorOrNil()
}

type Job struct {
	BranchID    keboola.BranchID
	ComponentID keboola.ComponentID
	ConfigID    keboola.ConfigID
	Tag         string

	id   keboola.JobID
	wait func() error
}

func NewJob(branchID keboola.BranchID, componentID keboola.ComponentID, configID keboola.ConfigID, tag string) *Job {
	return &Job{
		BranchID:    branchID,
		ComponentID: componentID,
		ConfigID:    configID,
		Tag:         tag,
	}
}

func (o *Job) Key() string {
	out := ""
	if o.BranchID > 0 {
		out += fmt.Sprintf("%d/", o.BranchID)
	}
	out += fmt.Sprintf("%s/%s", o.ComponentID, o.ConfigID)
	if len(o.Tag) > 0 {
		out += fmt.Sprintf("@%s", o.Tag)
	}
	return out
}

func (o *Job) Start(ctx context.Context, api *keboola.API, async bool, hasQueueV2 bool) error {
	if hasQueueV2 {
		job, err := api.NewCreateJobRequest(o.ComponentID).
			WithConfig(o.ConfigID).
			WithBranch(o.BranchID).
			WithTag(o.Tag).
			Send(ctx)
		if err != nil {
			return err
		}

		o.id = job.ID
		o.wait = func() error {
			err := api.WaitForQueueJob(ctx, job.ID)
			if err != nil {
				return err
			}
			return nil
		}
	} else {
		// nolint: staticcheck
		job, err := api.CreateOldQueueJobRequest(
			o.ComponentID,
			o.ConfigID,
			keboola.WithBranchID(o.BranchID),
			keboola.WithImageTag(o.Tag),
		).Send(ctx)
		if err != nil {
			return err
		}

		o.id = job.ID
		o.wait = func() error {
			// nolint: staticcheck
			err := api.WaitForOldQueueJob(ctx, job.ID)
			if err != nil {
				return err
			}
			return nil
		}
	}
	return nil
}

func (o *Job) Wait() error {
	return o.wait()
}
