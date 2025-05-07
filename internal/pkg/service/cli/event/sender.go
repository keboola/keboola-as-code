package event

import (
	"context"
	"fmt"
	"time"

	"github.com/keboola/keboola-sdk-go/v2/pkg/client"
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

const componentID = keboola.ComponentID("keboola.keboola-as-code")

type Sender struct {
	logger    log.Logger
	client    *keboola.AuthorizedAPI
	projectID keboola.ProjectID
}

func NewSender(logger log.Logger, client *keboola.AuthorizedAPI, projectID keboola.ProjectID) Sender {
	return Sender{logger: logger, client: client, projectID: projectID}
}

// SendCmdEvent sends failed event if an error occurred, otherwise it sends successful event.
func (s Sender) SendCmdEvent(ctx context.Context, cmdStart time.Time, errPtr *error, cmd string) {
	// Get error
	var err error
	if errPtr != nil {
		err = *errPtr
	}

	// Catch panic
	panicErr := recover()
	if panicErr != nil {
		err = errors.Errorf(`%s`, panicErr)
	}

	// Send successful event if no error
	if err == nil {
		msg := fmt.Sprintf(`%s command done.`, strhelper.FirstUpper(cmd))
		s.sendCmdSuccessfulEvent(ctx, cmdStart, cmd, msg)
		return
	}

	msg := fmt.Sprintf(`%s command failed.`, strhelper.FirstUpper(cmd))
	s.sendCmdFailedEvent(ctx, cmdStart, err, cmd, msg)

	// Throw panic
	if panicErr != nil {
		panic(panicErr)
	}
}

// sendCmdSuccessful send command successful event.
func (s Sender) sendCmdSuccessfulEvent(ctx context.Context, cmdStart time.Time, cmd, msg string) {
	duration := time.Since(cmdStart)
	params := map[string]any{
		"command": cmd,
	}
	results := map[string]any{
		"projectId": s.projectID,
	}
	event, err := s.client.CreateEventRequest(&keboola.Event{
		ComponentID: componentID,
		Type:        "info",
		Message:     msg,
		Duration:    client.DurationSeconds(duration),
		Params:      params,
		Results:     results,
	}).Send(ctx)
	if err == nil {
		s.logger.Debugf(ctx, "Sent \"%s\" successful event id: \"%s\"", cmd, event.ID)
	} else {
		s.logger.Warnf(ctx, "Cannot send \"%s\" successful event: %s", cmd, err)
	}
}

// sendCmdFailed send command failed event.
func (s Sender) sendCmdFailedEvent(ctx context.Context, cmdStart time.Time, err error, cmd, msg string) {
	duration := time.Since(cmdStart)
	params := map[string]any{
		"command": cmd,
	}
	results := map[string]any{
		"projectId": s.projectID,
		"error":     fmt.Sprintf("%s", err),
	}
	event, err := s.client.CreateEventRequest(&keboola.Event{
		ComponentID: componentID,
		Type:        "error",
		Message:     msg,
		Duration:    client.DurationSeconds(duration),
		Params:      params,
		Results:     results,
	}).Send(ctx)
	if err == nil {
		s.logger.Debugf(ctx, "Sent \"%s\" failed event id: \"%s\"", cmd, event.ID)
	} else {
		s.logger.Warnf(ctx, "Cannot send \"%s\" failed event: %s", cmd, err)
	}
}
