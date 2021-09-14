package event

import (
	"fmt"
	"time"

	"go.uber.org/zap"

	"keboola-as-code/src/remote"
)

func SendCmdSuccessfulEvent(cmdStart time.Time, logger *zap.SugaredLogger, api *remote.StorageApi, cmd, msg string) {
	duration := time.Since(cmdStart)
	params := map[string]interface{}{
		"command": cmd,
	}
	results := map[string]interface{}{
		"projectId": api.ProjectId(),
	}
	event, err := api.CreateEvent("info", msg, duration, params, results)
	if err == nil {
		logger.Debugf("Sent \"%s\" successful event id: \"%s\"", cmd, event.Id)
	} else {
		logger.Warnf("Cannot send \"%s\" successful event: %s", cmd, err)
	}
}

func SendCmdFailedEvent(cmdStart time.Time, logger *zap.SugaredLogger, api *remote.StorageApi, err error, cmd, msg string) {
	duration := time.Since(cmdStart)
	params := map[string]interface{}{
		"command": cmd,
	}
	results := map[string]interface{}{
		"projectId": api.ProjectId(),
		"error":     fmt.Sprintf("%s", err),
	}
	event, err := api.CreateEvent("error", msg, duration, params, results)
	if err == nil {
		logger.Debugf("Sent \"%s\" failed event id: \"%s\"", cmd, event.Id)
	} else {
		logger.Warnf("Cannot send \"%s\" failed event: %s", cmd, err)
	}
}
