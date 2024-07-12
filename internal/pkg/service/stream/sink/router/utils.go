package router

import (
	"net/http"
	"strconv"

	svcerrors "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/pipeline"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func resultStatusCode(status pipeline.RecordStatus, err error) int {
	switch {
	case err != nil:
		var withStatus svcerrors.WithStatusCode
		if errors.As(err, &withStatus) {
			return withStatus.StatusCode()
		}
		return http.StatusInternalServerError
	case status == pipeline.RecordProcessed:
		return http.StatusOK
	case status == pipeline.RecordAccepted:
		return http.StatusAccepted
	default:
		panic(errors.Errorf(`unexpected record status code %v`, status))
	}
}

func resultErrorName(err error) string {
	if err == nil {
		return ""
	}

	var withName svcerrors.WithName
	if errors.As(err, &withName) {
		return ErrorNamePrefix + withName.ErrorName()
	}

	return ErrorNamePrefix + "genericError"
}

func resultMessage(status pipeline.RecordStatus, err error) string {
	switch {
	case err != nil:
		var withMsg svcerrors.WithUserMessage
		if errors.As(err, &withMsg) {
			return withMsg.ErrorUserMessage()
		}
		return errors.Format(err, errors.FormatAsSentences())
	case status == pipeline.RecordProcessed:
		return "processed"
	case status == pipeline.RecordAccepted:
		return "accepted"
	default:
		panic(errors.Errorf(`unexpected record status code %v`, status))
	}
}

func aggregatedResultMessage(successful, all int) string {
	if all == 0 {
		return "No enabled sink found."
	}
	if successful == all {
		return "Successfully written to " + strconv.Itoa(successful) + "/" + strconv.Itoa(all) + " sinks."
	}
	return "Written to " + strconv.Itoa(successful) + "/" + strconv.Itoa(all) + " sinks."
}
