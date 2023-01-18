package event

import (
	"context"
	"fmt"
	"time"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/storageapi"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

const componentID = storageapi.ComponentID("keboola.keboola-buffer")

type Sender struct {
	logger log.Logger
	client client.Sender
}

func NewSender(logger log.Logger, client client.Sender) *Sender {
	return &Sender{logger: logger, client: client}
}

// SendCmdEvent sends failed event if an error occurred, otherwise it sends successful event.
func (s *Sender) SendCmdEvent(ctx context.Context, cmdStart time.Time, err error, cmd string, projectID int) {
	// Catch panic
	panicErr := recover()
	if panicErr != nil {
		err = errors.Errorf(`%s`, panicErr)
	}

	// Send successful event if no error
	if err == nil {
		msg := fmt.Sprintf(`%s command done.`, strhelper.FirstUpper(cmd))
		s.sendCmdSuccessfulEvent(ctx, cmdStart, cmd, msg, projectID)
		return
	}

	msg := fmt.Sprintf(`%s command failed.`, strhelper.FirstUpper(cmd))
	s.sendCmdFailedEvent(ctx, cmdStart, err, cmd, msg, projectID)

	// Throw panic
	if panicErr != nil {
		panic(panicErr)
	}
}

// sendCmdSuccessful send command successful event.
func (s *Sender) sendCmdSuccessfulEvent(ctx context.Context, cmdStart time.Time, cmd, msg string, projectID int) {
	duration := time.Since(cmdStart)
	params := map[string]interface{}{
		"command": cmd,
	}
	results := map[string]interface{}{
		"projectId": projectID,
	}
	event, err := storageapi.CreatEventRequest(&storageapi.Event{
		ComponentID: componentID,
		Type:        "info",
		Message:     msg,
		Duration:    client.DurationSeconds(duration),
		Params:      params,
		Results:     results,
	}).Send(ctx, s.client)
	if err == nil {
		s.logger.Debugf("Sent \"%s\" successful event id: \"%s\"", cmd, event.ID)
	} else {
		s.logger.Warnf("Cannot send \"%s\" successful event: %s", cmd, err)
	}
}

// sendCmdFailed send command failed event.
func (s *Sender) sendCmdFailedEvent(ctx context.Context, cmdStart time.Time, err error, cmd, msg string, projectID int) {
	duration := time.Since(cmdStart)
	params := map[string]interface{}{
		"command": cmd,
	}
	results := map[string]interface{}{
		"projectId": projectID,
		"error":     fmt.Sprintf("%s", err),
	}
	event, err := storageapi.CreatEventRequest(&storageapi.Event{
		ComponentID: componentID,
		Type:        "error",
		Message:     msg,
		Duration:    client.DurationSeconds(duration),
		Params:      params,
		Results:     results,
	}).Send(ctx, s.client)
	if err == nil {
		s.logger.Debugf("Sent \"%s\" failed event id: \"%s\"", cmd, event.ID)
	} else {
		s.logger.Warnf("Cannot send \"%s\" failed event: %s", cmd, err)
	}
}
