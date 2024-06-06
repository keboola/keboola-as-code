package service

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/rollback"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/task"
	api "github.com/keboola/keboola-as-code/internal/pkg/service/stream/api/gen/stream"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type taskConfig struct {
	Type      string
	Timeout   time.Duration
	ProjectID keboola.ProjectID
	ObjectKey fmt.Stringer
	Operation task.Fn
}

func (s *service) GetTask(ctx context.Context, d dependencies.ProjectRequestScope, payload *api.GetTaskPayload) (*api.Task, error) {
	t, err := s.tasks.GetTask(task.Key{ProjectID: d.ProjectID(), TaskID: payload.TaskID}).Do(ctx).ResultOrErr()
	if err != nil {
		return nil, err
	}

	return s.mapper.NewTaskResponse(t)
}

func (s *service) startTask(ctx context.Context, cfg taskConfig) (task.Task, error) {
	cfg.Type = "api." + cfg.Type

	objectKey := cfg.ObjectKey.String()
	projectIDPrefix := cfg.ProjectID.String() + "/"
	if strings.HasPrefix(objectKey, projectIDPrefix) {
		objectKey = strings.TrimPrefix(objectKey, projectIDPrefix)
	} else {
		return task.Task{}, errors.Errorf(`object key must start with the project ID "%s", found: "%s"`, projectIDPrefix, objectKey)
	}

	taskID := task.ID(cfg.Type + "/" + objectKey)
	return s.tasks.StartTask(ctx, task.Config{
		Type: cfg.Type,
		Key:  task.Key{ProjectID: cfg.ProjectID, TaskID: taskID},
		Lock: cfg.ObjectKey.String(),
		Context: func() (context.Context, context.CancelFunc) {
			return context.WithTimeout(context.WithoutCancel(ctx), cfg.Timeout)
		},
		Operation: func(ctx context.Context, logger log.Logger) task.Result {
			rb := rollback.New(logger)
			ctx = rollback.ContextWith(ctx, rb)
			result := cfg.Operation(ctx, logger)
			rb.InvokeIfErr(ctx, &result.Error)
			return result
		},
	})
}
