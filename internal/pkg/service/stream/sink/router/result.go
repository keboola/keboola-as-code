package router

import (
	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
)

type SourcesResult struct {
	StatusCode      int            `json:"statusCode"`
	ErrorName       string         `json:"error,omitempty"`
	Message         string         `json:"message"`
	Sources         []SourceResult `json:"sources,omitempty"`
	AllSinks        int            `json:"-"`
	SuccessfulSinks int            `json:"-"`
	FailedSinks     int            `json:"-"`
}

type SourceResult struct {
	ProjectID       keboola.ProjectID `json:"projectId"`
	SourceID        key.SourceID      `json:"sourceId"`
	BranchID        keboola.BranchID  `json:"branchId"`
	StatusCode      int               `json:"statusCode"`
	ErrorName       string            `json:"error,omitempty"`
	Message         string            `json:"message"`
	Sinks           []SinkResult      `json:"sinks,omitempty"`
	AllSinks        int               `json:"-"`
	SuccessfulSinks int               `json:"-"`
	FailedSinks     int               `json:"-"`
}

type SinkResult struct {
	SinkID     key.SinkID `json:"sinkId"`
	StatusCode int        `json:"statusCode"`
	ErrorName  string     `json:"error,omitempty"`
	Message    string     `json:"message"`
}
