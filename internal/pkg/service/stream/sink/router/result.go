package router

import (
	"net/http"
	"slices"
	"strconv"
	"strings"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	svcerrors "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/pipeline"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type SourcesResult struct {
	StatusCode      int             `json:"statusCode"`
	ErrorName       string          `json:"error,omitempty"`
	Message         string          `json:"message"`
	Sources         []*SourceResult `json:"sources,omitempty"`
	AllSinks        int             `json:"-"`
	SuccessfulSinks int             `json:"-"`
	FailedSinks     int             `json:"-"`
	finalized       bool
}

type SourceResult struct {
	ProjectID       keboola.ProjectID `json:"projectId"`
	SourceID        key.SourceID      `json:"sourceId"`
	BranchID        keboola.BranchID  `json:"branchId"`
	StatusCode      int               `json:"statusCode"`
	ErrorName       string            `json:"error,omitempty"`
	Message         string            `json:"message"`
	Sinks           []*SinkResult     `json:"sinks,omitempty"`
	AllSinks        int               `json:"-"`
	SuccessfulSinks int               `json:"-"`
	FailedSinks     int               `json:"-"`
	finalized       bool
}

type SinkResult struct {
	SinkID     key.SinkID `json:"sinkId"`
	StatusCode int        `json:"statusCode"`
	ErrorName  string     `json:"error,omitempty"`
	Message    string     `json:"message"`
	status     pipeline.RecordStatus
	error      error
	finalized  bool
}

// Finalize result strings only when they are really needed - verbose mode or an error.
func (r *SourcesResult) Finalize() {
	if r.finalized {
		return
	}

	r.finalized = true

	// Error name
	if r.FailedSinks > 0 {
		r.ErrorName = ErrorNamePrefix + "writeFailed"
	}

	// Message
	switch {
	case r.AllSinks == 0:
		r.Message = "No enabled sink found."
	case r.SuccessfulSinks == r.AllSinks:
		var b strings.Builder
		b.WriteString("Successfully written to ")
		b.WriteString(strconv.Itoa(r.AllSinks))
		b.WriteString("/")
		b.WriteString(strconv.Itoa(r.AllSinks))
		b.WriteString(" sinks.")
		r.Message = b.String()
	default:
		var b strings.Builder
		b.WriteString("Written to ")
		b.WriteString(strconv.Itoa(r.SuccessfulSinks))
		b.WriteString("/")
		b.WriteString(strconv.Itoa(r.AllSinks))
		b.WriteString(" sinks.")
		r.Message = b.String()
	}

	// Sort sources results
	slices.SortStableFunc(r.Sources, func(a, b *SourceResult) int {
		return strings.Compare(a.BranchID.String(), b.BranchID.String())
	})

	// Finalize sources results
	for _, s := range r.Sources {
		s.Finalize()
	}
}

// Finalize result strings only when they are really needed - verbose mode or an error.
func (r *SourceResult) Finalize() {
	if r.finalized {
		return
	}

	r.finalized = true

	// Error name
	if r.FailedSinks > 0 {
		r.ErrorName = ErrorNamePrefix + "writeFailed"
	}

	// Message
	switch {
	case r.AllSinks == 0:
		r.Message = "No enabled sink found."
	case r.SuccessfulSinks == r.AllSinks:
		var b strings.Builder
		b.WriteString("Successfully written to ")
		b.WriteString(strconv.Itoa(r.AllSinks))
		b.WriteString("/")
		b.WriteString(strconv.Itoa(r.AllSinks))
		b.WriteString(" sinks.")
		r.Message = b.String()
	default:
		var b strings.Builder
		b.WriteString("Written to ")
		b.WriteString(strconv.Itoa(r.SuccessfulSinks))
		b.WriteString("/")
		b.WriteString(strconv.Itoa(r.AllSinks))
		b.WriteString(" sinks.")
		r.Message = b.String()
	}

	// Sort sinks results
	slices.SortStableFunc(r.Sinks, func(a, b *SinkResult) int {
		return strings.Compare(a.SinkID.String(), b.SinkID.String())
	})

	// Finalize sinks results
	for _, s := range r.Sinks {
		s.Finalize()
	}
}

// Finalize result strings only when they are really needed - verbose mode or an error.
func (r *SinkResult) Finalize() {
	if r.finalized {
		return
	}

	r.finalized = true

	// Error name
	r.ErrorName = resultErrorName(r.error)

	// Message
	switch {
	case r.error != nil:
		var withMsg svcerrors.WithUserMessage
		if errors.As(r.error, &withMsg) {
			r.Message = withMsg.ErrorUserMessage()
		} else {
			r.Message = errors.Format(r.error, errors.FormatAsSentences())
		}
	case r.status == pipeline.RecordProcessed:
		r.Message = "processed"
	case r.status == pipeline.RecordAccepted:
		r.Message = "accepted"
	default:
		panic(errors.Errorf(`unexpected record status code %v`, r.status))
	}
}

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
