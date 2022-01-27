package eventsender

import (
	"fmt"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/api/storageapi"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/strhelper"
)

type Sender struct {
	logger     log.Logger
	storageApi *storageapi.Api
}

func New(logger log.Logger, storageApi *storageapi.Api) *Sender {
	return &Sender{logger: logger, storageApi: storageApi}
}

// SendCmdEvent sends failed event if an error occurred, otherwise it sends successful event.
func (s *Sender) SendCmdEvent(cmdStart time.Time, err error, cmd string) {
	// Catch panic
	panicErr := recover()
	if panicErr != nil {
		err = fmt.Errorf(`%s`, panicErr)
	}

	// Send successful event if no error
	if err == nil {
		msg := fmt.Sprintf(`%s command done.`, strhelper.FirstUpper(cmd))
		s.sendCmdSuccessfulEvent(cmdStart, cmd, msg)
		return
	}

	msg := fmt.Sprintf(`%s command failed.`, strhelper.FirstUpper(cmd))
	s.sendCmdFailedEvent(cmdStart, err, cmd, msg)

	// Throw panic
	if panicErr != nil {
		panic(panicErr)
	}
}

// sendCmdSuccessful send command successful event.
func (s *Sender) sendCmdSuccessfulEvent(cmdStart time.Time, cmd, msg string) {
	duration := time.Since(cmdStart)
	params := map[string]interface{}{
		"command": cmd,
	}
	results := map[string]interface{}{
		"projectId": s.storageApi.ProjectId(),
	}
	event, err := s.storageApi.CreateEvent("info", msg, duration, params, results)
	if err == nil {
		s.logger.Debugf("Sent \"%s\" successful event id: \"%s\"", cmd, event.Id)
	} else {
		s.logger.Warnf("Cannot send \"%s\" successful event: %s", cmd, err)
	}
}

// sendCmdFailed send command failed event.
func (s *Sender) sendCmdFailedEvent(cmdStart time.Time, err error, cmd, msg string) {
	// Log event
	duration := time.Since(cmdStart)
	params := map[string]interface{}{
		"command": cmd,
	}
	results := map[string]interface{}{
		"projectId": s.storageApi.ProjectId(),
		"error":     fmt.Sprintf("%s", err),
	}
	event, err := s.storageApi.CreateEvent("error", msg, duration, params, results)
	if err == nil {
		s.logger.Debugf("Sent \"%s\" failed event id: \"%s\"", cmd, event.Id)
	} else {
		s.logger.Warnf("Cannot send \"%s\" failed event: %s", cmd, err)
	}
}
