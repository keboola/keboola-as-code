package run

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/sasha-s/go-deadlock"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type dependencies interface {
	KeboolaProjectAPI() *keboola.AuthorizedAPI
	ProjectFeatures() keboola.FeaturesMap
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
}

type RunOptions struct {
	Jobs    []*Job
	Async   bool
	Timeout time.Duration
}

func Run(ctx context.Context, o RunOptions, d dependencies) (err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.project.remote.job.run")
	defer span.End(&err)

	timeoutCtx, cancel := context.WithTimeoutCause(ctx, o.Timeout, errors.New("remote job timeout"))
	defer cancel()

	queue := &JobQueue{
		async:          o.Async,
		ctx:            timeoutCtx,
		api:            d.KeboolaProjectAPI(),
		logger:         d.Logger(),
		wg:             &sync.WaitGroup{},
		err:            errors.NewMultiError(),
		remaining:      map[string]bool{},
		remainingMutex: &deadlock.Mutex{},
		done:           make(chan struct{}),
	}

	if len(o.Jobs) > 1 {
		queue.logger.Infof(ctx, "Starting %d jobs.", len(o.Jobs))
	} else {
		queue.logger.Info(ctx, "Starting job.")
	}

	// dispatch jobs in parallel
	for _, job := range o.Jobs {
		queue.dispatch(job)
	}

	queue.startLogRemaining()
	err = queue.wait()
	queue.stopLogRemaining()

	if err != nil {
		queue.logger.Error(ctx, "Some jobs failed, see below.")
		return err
	} else {
		if !o.Async {
			queue.logger.Info(ctx, "Finished all jobs.")
		} else {
			queue.logger.Info(ctx, "Started all jobs.")
		}
		return nil
	}
}

type JobQueue struct {
	async bool

	ctx    context.Context
	api    *keboola.AuthorizedAPI
	logger log.Logger

	wg  *sync.WaitGroup
	err errors.MultiError

	remaining      map[string]bool
	remainingMutex *deadlock.Mutex

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
				q.logger.Infof(q.ctx, `Waiting for "%s"`, strings.Join(q.getRemainingJobs(), `", "`))
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
	q.logger.Infof(q.ctx, "Started job \"%s\" using config \"%s\"", job.id, job.Key())

	q.remainingMutex.Lock()
	q.remaining[string(job.id)] = true
	q.remainingMutex.Unlock()
}

func (q *JobQueue) finished(job *Job) {
	q.remainingMutex.Lock()
	q.remaining[string(job.id)] = false
	q.remainingMutex.Unlock()

	q.logger.Infof(q.ctx, "Finished job \"%s\"", job.id)
}

func (q *JobQueue) dispatch(job *Job) {
	q.wg.Add(1)

	go func() {
		defer q.wg.Done()

		if err := job.Start(q.ctx, q.api); err != nil {
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

func (o *Job) Start(ctx context.Context, api *keboola.AuthorizedAPI) error {
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

	return nil
}

func (o *Job) Wait() error {
	return o.wait()
}
