package service

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/gen/buffer"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/rollback"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/task"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	receiverCreateTaskType = "receiver.create"
)

func (s *service) CreateReceiver(d dependencies.ForProjectRequest, payload *buffer.CreateReceiverPayload) (res *buffer.Task, err error) {
	ctx, str := d.RequestCtx(), d.Store()

	receiver, err := s.mapper.CreateReceiverModel(keboola.ProjectID(d.ProjectID()), idgenerator.ReceiverSecret(), *payload)
	if err != nil {
		return nil, err
	}

	// Check if receiver does not exist and the receivers count limit is not reached.
	err = str.CheckCreateReceiver(ctx, receiver.ReceiverKey)
	if err != nil {
		return nil, err
	}

	taskKey := task.Key{
		ProjectID: receiver.ProjectID,
		TaskID: task.ID(strings.Join([]string{
			receiver.ReceiverID.String(),
			receiverCreateTaskType,
		}, "/")),
	}

	t, err := d.TaskNode().StartTask(task.Config{
		Type: receiverCreateTaskType,
		Key:  taskKey,
		Context: func() (context.Context, context.CancelFunc) {
			return context.WithTimeout(context.Background(), 5*time.Minute)
		},
		Operation: func(ctx context.Context, logger log.Logger) (task task.Result, err error) {
			rb := rollback.New(s.logger)
			defer rb.InvokeIfErr(ctx, &err)

			if err := s.createResourcesForReceiver(ctx, d, rb, &receiver); err != nil {
				return "", err
			}

			if err := str.CreateReceiver(ctx, receiver); err != nil {
				return "", err
			}
			return "receiver created", nil
		},
	})
	if err != nil {
		return nil, err
	}
	return s.mapper.TaskPayload(t), nil
}

func (s *service) UpdateReceiver(d dependencies.ForProjectRequest, payload *buffer.UpdateReceiverPayload) (res *buffer.Receiver, err error) {
	ctx, str := d.RequestCtx(), d.Store()

	rb := rollback.New(s.logger)
	defer rb.InvokeIfErr(ctx, &err)

	receiverKey := key.ReceiverKey{ProjectID: keboola.ProjectID(d.ProjectID()), ReceiverID: payload.ReceiverID}
	err = str.UpdateReceiver(ctx, receiverKey, func(receiver model.ReceiverBase) (model.ReceiverBase, error) {
		return s.mapper.UpdateReceiverModel(receiver, *payload)
	})

	if err != nil {
		return nil, err
	}

	return s.GetReceiver(d, &buffer.GetReceiverPayload{ReceiverID: receiverKey.ReceiverID})
}

func (s *service) GetReceiver(d dependencies.ForProjectRequest, payload *buffer.GetReceiverPayload) (res *buffer.Receiver, err error) {
	ctx, str := d.RequestCtx(), d.Store()

	receiverKey := key.ReceiverKey{ProjectID: keboola.ProjectID(d.ProjectID()), ReceiverID: payload.ReceiverID}
	receiver, err := str.GetReceiver(ctx, receiverKey)
	if err != nil {
		return nil, err
	}

	return s.mapper.ReceiverPayload(receiver), nil
}

func (s *service) ListReceivers(d dependencies.ForProjectRequest, _ *buffer.ListReceiversPayload) (res *buffer.ReceiversList, err error) {
	ctx, str := d.RequestCtx(), d.Store()

	receivers, err := str.ListReceivers(ctx, keboola.ProjectID(d.ProjectID()))
	if err != nil {
		return nil, errors.Wrapf(err, "failed to list receivers in the project")
	}

	return &buffer.ReceiversList{Receivers: s.mapper.ReceiversPayload(receivers)}, nil
}

func (s *service) DeleteReceiver(d dependencies.ForProjectRequest, payload *buffer.DeleteReceiverPayload) (err error) {
	ctx, str := d.RequestCtx(), d.Store()
	receiverKey := key.ReceiverKey{ProjectID: keboola.ProjectID(d.ProjectID()), ReceiverID: payload.ReceiverID}
	return str.DeleteReceiver(ctx, receiverKey)
}

func (s *service) createResourcesForReceiver(ctx context.Context, d dependencies.ForProjectRequest, rb rollback.Builder, receiver *model.Receiver) error {
	// Buket is required by token and table
	if err := d.TableManager().EnsureBucketsExist(ctx, rb, receiver); err != nil {
		return err
	}

	// The following operations can be performed in parallel
	rb = rb.AddParallel()
	errs := errors.NewMultiError()
	wg := &sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := d.TableManager().EnsureTablesExist(ctx, rb, receiver); err != nil {
			errs.Append(err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := d.TokenManager().CreateTokens(ctx, rb, receiver); err != nil {
			errs.Append(err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := d.FileManager().CreateFilesForReceiver(ctx, rb, receiver); err != nil {
			errs.Append(err)
		}
	}()

	wg.Wait()
	return errs.ErrorOrNil()
}
