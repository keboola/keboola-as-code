// Code generated by goa v3.19.1, DO NOT EDIT.
//
// HTTP request path constructors for the stream service.
//
// Command:
// $ goa gen github.com/keboola/keboola-as-code/api/stream --output
// ./internal/pkg/service/stream/api

package server

import (
	"fmt"
)

// APIRootIndexStreamPath returns the URL path to the stream service ApiRootIndex HTTP endpoint.
func APIRootIndexStreamPath() string {
	return "/"
}

// APIVersionIndexStreamPath returns the URL path to the stream service ApiVersionIndex HTTP endpoint.
func APIVersionIndexStreamPath() string {
	return "/v1"
}

// HealthCheckStreamPath returns the URL path to the stream service HealthCheck HTTP endpoint.
func HealthCheckStreamPath() string {
	return "/health-check"
}

// CreateSourceStreamPath returns the URL path to the stream service CreateSource HTTP endpoint.
func CreateSourceStreamPath(branchID string) string {
	return fmt.Sprintf("/v1/branches/%v/sources", branchID)
}

// UpdateSourceStreamPath returns the URL path to the stream service UpdateSource HTTP endpoint.
func UpdateSourceStreamPath(branchID string, sourceID string) string {
	return fmt.Sprintf("/v1/branches/%v/sources/%v", branchID, sourceID)
}

// ListSourcesStreamPath returns the URL path to the stream service ListSources HTTP endpoint.
func ListSourcesStreamPath(branchID string) string {
	return fmt.Sprintf("/v1/branches/%v/sources", branchID)
}

// GetSourceStreamPath returns the URL path to the stream service GetSource HTTP endpoint.
func GetSourceStreamPath(branchID string, sourceID string) string {
	return fmt.Sprintf("/v1/branches/%v/sources/%v", branchID, sourceID)
}

// DeleteSourceStreamPath returns the URL path to the stream service DeleteSource HTTP endpoint.
func DeleteSourceStreamPath(branchID string, sourceID string) string {
	return fmt.Sprintf("/v1/branches/%v/sources/%v", branchID, sourceID)
}

// GetSourceSettingsStreamPath returns the URL path to the stream service GetSourceSettings HTTP endpoint.
func GetSourceSettingsStreamPath(branchID string, sourceID string) string {
	return fmt.Sprintf("/v1/branches/%v/sources/%v/settings", branchID, sourceID)
}

// UpdateSourceSettingsStreamPath returns the URL path to the stream service UpdateSourceSettings HTTP endpoint.
func UpdateSourceSettingsStreamPath(branchID string, sourceID string) string {
	return fmt.Sprintf("/v1/branches/%v/sources/%v/settings", branchID, sourceID)
}

// TestSourceStreamPath returns the URL path to the stream service TestSource HTTP endpoint.
func TestSourceStreamPath(branchID string, sourceID string) string {
	return fmt.Sprintf("/v1/branches/%v/sources/%v/test", branchID, sourceID)
}

// SourceStatisticsClearStreamPath returns the URL path to the stream service SourceStatisticsClear HTTP endpoint.
func SourceStatisticsClearStreamPath(branchID string, sourceID string) string {
	return fmt.Sprintf("/v1/branches/%v/sources/%v/statistics/clear", branchID, sourceID)
}

// DisableSourceStreamPath returns the URL path to the stream service DisableSource HTTP endpoint.
func DisableSourceStreamPath(branchID string, sourceID string) string {
	return fmt.Sprintf("/v1/branches/%v/sources/%v/disable", branchID, sourceID)
}

// EnableSourceStreamPath returns the URL path to the stream service EnableSource HTTP endpoint.
func EnableSourceStreamPath(branchID string, sourceID string) string {
	return fmt.Sprintf("/v1/branches/%v/sources/%v/enable", branchID, sourceID)
}

// ListSourceVersionsStreamPath returns the URL path to the stream service ListSourceVersions HTTP endpoint.
func ListSourceVersionsStreamPath(branchID string, sourceID string) string {
	return fmt.Sprintf("/v1/branches/%v/sources/%v/versions", branchID, sourceID)
}

// CreateSinkStreamPath returns the URL path to the stream service CreateSink HTTP endpoint.
func CreateSinkStreamPath(branchID string, sourceID string) string {
	return fmt.Sprintf("/v1/branches/%v/sources/%v/sinks", branchID, sourceID)
}

// GetSinkStreamPath returns the URL path to the stream service GetSink HTTP endpoint.
func GetSinkStreamPath(branchID string, sourceID string, sinkID string) string {
	return fmt.Sprintf("/v1/branches/%v/sources/%v/sinks/%v", branchID, sourceID, sinkID)
}

// GetSinkSettingsStreamPath returns the URL path to the stream service GetSinkSettings HTTP endpoint.
func GetSinkSettingsStreamPath(branchID string, sourceID string, sinkID string) string {
	return fmt.Sprintf("/v1/branches/%v/sources/%v/sinks/%v/settings", branchID, sourceID, sinkID)
}

// UpdateSinkSettingsStreamPath returns the URL path to the stream service UpdateSinkSettings HTTP endpoint.
func UpdateSinkSettingsStreamPath(branchID string, sourceID string, sinkID string) string {
	return fmt.Sprintf("/v1/branches/%v/sources/%v/sinks/%v/settings", branchID, sourceID, sinkID)
}

// ListSinksStreamPath returns the URL path to the stream service ListSinks HTTP endpoint.
func ListSinksStreamPath(branchID string, sourceID string) string {
	return fmt.Sprintf("/v1/branches/%v/sources/%v/sinks", branchID, sourceID)
}

// UpdateSinkStreamPath returns the URL path to the stream service UpdateSink HTTP endpoint.
func UpdateSinkStreamPath(branchID string, sourceID string, sinkID string) string {
	return fmt.Sprintf("/v1/branches/%v/sources/%v/sinks/%v", branchID, sourceID, sinkID)
}

// DeleteSinkStreamPath returns the URL path to the stream service DeleteSink HTTP endpoint.
func DeleteSinkStreamPath(branchID string, sourceID string, sinkID string) string {
	return fmt.Sprintf("/v1/branches/%v/sources/%v/sinks/%v", branchID, sourceID, sinkID)
}

// SinkStatisticsTotalStreamPath returns the URL path to the stream service SinkStatisticsTotal HTTP endpoint.
func SinkStatisticsTotalStreamPath(branchID string, sourceID string, sinkID string) string {
	return fmt.Sprintf("/v1/branches/%v/sources/%v/sinks/%v/statistics/total", branchID, sourceID, sinkID)
}

// SinkStatisticsFilesStreamPath returns the URL path to the stream service SinkStatisticsFiles HTTP endpoint.
func SinkStatisticsFilesStreamPath(branchID string, sourceID string, sinkID string) string {
	return fmt.Sprintf("/v1/branches/%v/sources/%v/sinks/%v/statistics/files", branchID, sourceID, sinkID)
}

// SinkStatisticsClearStreamPath returns the URL path to the stream service SinkStatisticsClear HTTP endpoint.
func SinkStatisticsClearStreamPath(branchID string, sourceID string, sinkID string) string {
	return fmt.Sprintf("/v1/branches/%v/sources/%v/sinks/%v/statistics/clear", branchID, sourceID, sinkID)
}

// DisableSinkStreamPath returns the URL path to the stream service DisableSink HTTP endpoint.
func DisableSinkStreamPath(branchID string, sourceID string, sinkID string) string {
	return fmt.Sprintf("/v1/branches/%v/sources/%v/sinks/%v/disable", branchID, sourceID, sinkID)
}

// EnableSinkStreamPath returns the URL path to the stream service EnableSink HTTP endpoint.
func EnableSinkStreamPath(branchID string, sourceID string, sinkID string) string {
	return fmt.Sprintf("/v1/branches/%v/sources/%v/sinks/%v/enable", branchID, sourceID, sinkID)
}

// GetTaskStreamPath returns the URL path to the stream service GetTask HTTP endpoint.
func GetTaskStreamPath(taskID string) string {
	return fmt.Sprintf("/v1/tasks/%v", taskID)
}

// AggregationSourcesStreamPath returns the URL path to the stream service AggregationSources HTTP endpoint.
func AggregationSourcesStreamPath(branchID string) string {
	return fmt.Sprintf("/v1/branches/%v/aggregation/sources", branchID)
}
