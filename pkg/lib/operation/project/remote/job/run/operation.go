package run

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/keboola/go-client/pkg/keboola"
	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type dependencies interface {
	KeboolaProjectAPI() *keboola.API
	Logger() log.Logger
	Tracer() trace.Tracer
}

func Run(ctx context.Context, o RunOptions, d dependencies) (err error) {
	ctx, span := d.Tracer().Start(ctx, "kac.lib.operation.project.remote.job.run")
	defer telemetry.EndSpan(span, &err)

	logger := d.Logger()
	api := d.KeboolaProjectAPI()

	timeoutCtx, cancel := context.WithTimeout(ctx, o.Timeout)
	defer cancel()

	// remaining stores keys of jobs which are still running
	remaining := map[string]bool{}
	remainingMutex := &sync.Mutex{}
	jobErrors := errors.NewMultiError()
	wg := &sync.WaitGroup{}

	if len(o.Jobs) > 1 {
		logger.Infof("Starting %d jobs", len(o.Jobs))
	} else {
		logger.Info("Starting 1 job")
	}

	// dispatch jobs in parallel
	for _, job := range o.Jobs {
		job := job

		remaining[job.Key] = true
		wg.Add(1)
		go dispatchJob(timeoutCtx, api, &job, wg, jobErrors, remaining, remainingMutex)
	}

	done := make(chan struct{})
	go logRemaining(timeoutCtx, done, remaining, remainingMutex, logger)

	wg.Wait()
	done <- struct{}{}
	err = jobErrors.ErrorOrNil()
	if err != nil {
		logger.Error("Some jobs failed, see below")
		return err
	} else {
		if !o.Async {
			logger.Info("Finished running all jobs")
		} else {
			logger.Info("Started all jobs")
		}
		return nil
	}
}

func dispatchJob(
	ctx context.Context,
	api *keboola.API,
	job *RunJob,
	wg *sync.WaitGroup,
	err errors.MultiError,
	remaining map[string]bool,
	remainingMutex *sync.Mutex,
) {
	defer wg.Done()

	if e := job.Run(ctx, api); e != nil {
		err.Append(e)
	}

	remainingMutex.Lock()
	remaining[job.Key] = false
	remainingMutex.Unlock()
}

func logRemaining(
	ctx context.Context,
	done chan struct{},
	remaining map[string]bool,
	remainingMutex *sync.Mutex,
	logger log.Logger,
) {
	for {
		select {
		case <-ctx.Done():
			break
		case <-done:
			break
		case <-time.After(time.Second * 5):
			remainingMutex.Lock()
			remainingJobs := []string{}
			for k, v := range remaining {
				if v {
					remainingJobs = append(remainingJobs, k)
				}
			}
			remainingMutex.Unlock()

			logger.Infof("Waiting for %s", strings.Join(remainingJobs, ", "))
		}
	}
}

type RunJob struct {
	Key         string
	BranchID    keboola.BranchID
	ComponentID keboola.ComponentID
	ConfigID    keboola.ConfigID
	HasQueueV2  bool
	Async       bool
}

func (o *RunJob) Run(ctx context.Context, api *keboola.API) error {
	if o.HasQueueV2 {
		job, err := api.CreateQueueJobRequest(o.ComponentID, o.ConfigID).Send(ctx)
		if err != nil {
			return err
		}

		if !o.Async {
			err = api.WaitForQueueJob(ctx, job)
			if err != nil {
				return err
			}
		}
	} else {
		job, err := api.CreateOldQueueJobRequest(o.ComponentID, o.ConfigID, keboola.WithBranchID(o.BranchID)).Send(ctx)
		if err != nil {
			return err
		}

		if !o.Async {
			err = api.WaitForOldQueueJob(ctx, job.ID)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

type RunOptions struct {
	Jobs    []RunJob
	Async   bool
	Timeout time.Duration
}

func (o *RunOptions) Parse(opts *options.Options, args []string, hasQueueV2 bool) error {
	o.Async = opts.GetBool("async")

	timeout, err := time.ParseDuration(opts.GetString("timeout"))
	if err != nil {
		return err
	}
	o.Timeout = timeout

	jobIndex := map[string]int{}
	invalidArgs := errors.NewMultiError()
	for _, arg := range args {
		// parse [branchID]/componentID/configID

		parts := strings.Split(arg, "/")
		if len(parts) < 2 || len(parts) > 3 {
			invalidArgs.Append(errors.Errorf(`invalid job format "%s"`, arg))
			continue
		}

		var branchID keboola.BranchID
		if len(parts) == 3 {
			value, err := strconv.Atoi(parts[0])
			if err != nil {
				invalidArgs.Append(errors.Errorf(`invalid branch ID "%s" in job "%s"`, parts[0], arg))
				continue
			}
			branchID = keboola.BranchID(value)
		}
		componentID := keboola.ComponentID(parts[len(parts)-2])
		configID := keboola.ConfigID(parts[len(parts)-1])

		index, ok := jobIndex[arg]
		if !ok {
			jobIndex[arg] = 1
			index = 0
		} else {
			jobIndex[arg] += 1
		}
		o.Jobs = append(o.Jobs, RunJob{
			Key:         arg + fmt.Sprintf(" (%d)", index),
			BranchID:    branchID,
			ComponentID: componentID,
			ConfigID:    configID,
			HasQueueV2:  hasQueueV2,
			Async:       o.Async,
		})
	}

	return invalidArgs.ErrorOrNil()
}
