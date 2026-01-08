//nolint:tagliatelle // RFC specifies snake_case for JSON output in twin format.
package twinformat

import (
	"time"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/llm/twinformat/configparser"
)

// Type aliases for types defined in configparser package.
type (
	TransformationConfig = configparser.TransformationConfig
	StorageMapping       = configparser.StorageMapping
	CodeBlock            = configparser.CodeBlock
	Code                 = configparser.Code
	ComponentConfig      = configparser.ComponentConfig
)

// ProjectData holds all fetched data from Keboola APIs.
type ProjectData struct {
	ProjectID             keboola.ProjectID
	BranchID              keboola.BranchID
	Buckets               []*keboola.Bucket
	Tables                []*keboola.Table
	Jobs                  []*keboola.QueueJob
	TransformationConfigs []*configparser.TransformationConfig
	ComponentConfigs      []*configparser.ComponentConfig
	Components            []*keboola.ComponentWithConfigs // All components with their metadata
	FetchedAt             time.Time
}

// TwinTable represents a table in the twin format output.
type TwinTable struct {
	UID           string             `json:"uid"`
	Name          string             `json:"name"`
	Type          string             `json:"type"`
	Bucket        string             `json:"bucket"`
	Source        string             `json:"source"`
	Description   string             `json:"description,omitempty"`
	Columns       []string           `json:"columns,omitempty"`
	PrimaryKey    []string           `json:"primary_key,omitempty"`
	RowsCount     uint64             `json:"rows_count,omitempty"`
	DataSizeBytes uint64             `json:"data_size_bytes,omitempty"`
	Dependencies  *TableDependencies `json:"dependencies"`
}

// TableDependencies represents lineage dependencies for a table.
type TableDependencies struct {
	ConsumedBy []string `json:"consumed_by"`
	ProducedBy []string `json:"produced_by"`
}

// TwinTransformation represents a transformation in the twin format output.
type TwinTransformation struct {
	UID          string                      `json:"uid"`
	Name         string                      `json:"name"`
	Type         string                      `json:"type"`
	Platform     string                      `json:"platform"`
	ComponentID  string                      `json:"component_id"`
	ConfigID     string                      `json:"config_id,omitempty"`
	IsDisabled   bool                        `json:"is_disabled"`
	Description  string                      `json:"description,omitempty"`
	OriginalPath string                      `json:"original_path,omitempty"`
	Dependencies *TransformationDependencies `json:"dependencies"`
	JobExecution *JobExecution               `json:"job_execution,omitempty"`
}

// TransformationDependencies represents lineage dependencies for a transformation.
type TransformationDependencies struct {
	Consumes []string `json:"consumes"`
	Produces []string `json:"produces"`
}

// JobExecution represents the last job execution status for a transformation.
type JobExecution struct {
	LastRunTime     string `json:"last_run_time,omitempty"`
	LastRunStatus   string `json:"last_run_status,omitempty"`
	JobReference    string `json:"job_reference,omitempty"`
	DurationSeconds int    `json:"duration_seconds,omitempty"`
	LastError       string `json:"last_error,omitempty"`
}

// TwinJob represents a job in the twin format output.
type TwinJob struct {
	ID              string `json:"id"`
	Status          string `json:"status"`
	ComponentID     string `json:"component_id"`
	ConfigID        string `json:"config_id,omitempty"`
	OperationName   string `json:"operation_name,omitempty"`
	StartTime       string `json:"start_time,omitempty"`
	EndTime         string `json:"end_time,omitempty"`
	DurationSeconds int    `json:"duration_seconds,omitempty"`
	ErrorMessage    string `json:"error_message,omitempty"`
}

// TwinBucket represents a bucket in the twin format output.
type TwinBucket struct {
	Name       string   `json:"name"`
	Source     string   `json:"source"`
	TableCount int      `json:"table_count"`
	Tables     []string `json:"tables"`
}

// BucketIndex represents the buckets/index.json structure.
type BucketIndex struct {
	Comment         string                  `json:"_comment"`
	Purpose         string                  `json:"_purpose"`
	UpdateFrequency string                  `json:"_update_frequency"`
	TotalBuckets    int                     `json:"total_buckets"`
	BySource        map[string]*SourceStats `json:"by_source"`
	Buckets         []*TwinBucket           `json:"buckets"`
}

// SourceStats represents statistics for a source.
type SourceStats struct {
	Count       int `json:"count"`
	TotalTables int `json:"total_tables"`
}

// TransformationIndex represents the transformations/index.json structure.
type TransformationIndex struct {
	Comment              string                      `json:"_comment"`
	Purpose              string                      `json:"_purpose"`
	UpdateFrequency      string                      `json:"_update_frequency"`
	TotalTransformations int                         `json:"total_transformations"`
	ByPlatform           map[string]int              `json:"by_platform"`
	Transformations      []*TransformationIndexEntry `json:"transformations"`
}

// TransformationIndexEntry represents a transformation entry in the index.
type TransformationIndexEntry struct {
	UID           string `json:"uid"`
	Name          string `json:"name"`
	Platform      string `json:"platform"`
	IsDisabled    bool   `json:"is_disabled"`
	InputCount    int    `json:"input_count"`
	OutputCount   int    `json:"output_count"`
	LastRunTime   string `json:"last_run_time,omitempty"`
	LastRunStatus string `json:"last_run_status,omitempty"`
	JobReference  string `json:"job_reference,omitempty"`
}

// JobsIndex represents the jobs/index.json structure.
type JobsIndex struct {
	Comment         string                  `json:"_comment"`
	Purpose         string                  `json:"_purpose"`
	UpdateFrequency string                  `json:"_update_frequency"`
	TotalJobs       int                     `json:"total_jobs"`
	RecentJobsCount int                     `json:"recent_jobs_count"`
	ByStatus        map[string]int          `json:"by_status"`
	ByOperation     map[string]int          `json:"by_operation"`
	Transformations *TransformationJobStats `json:"transformations"`
	RetentionPolicy *RetentionPolicy        `json:"retention_policy"`
}

// TransformationJobStats represents transformation job statistics.
type TransformationJobStats struct {
	Comment               string                     `json:"_comment,omitempty"`
	TotalRuns             int                        `json:"total_runs"`
	ByPlatform            map[string]int             `json:"by_platform"`
	RecentTransformations []*RecentTransformationJob `json:"recent_transformations"`
}

// RecentTransformationJob represents a recent transformation job.
type RecentTransformationJob struct {
	JobID           string `json:"job_id"`
	Transformation  string `json:"transformation"`
	ComponentID     string `json:"component_id"`
	Status          string `json:"status"`
	CompletedTime   string `json:"completed_time,omitempty"`
	DurationSeconds int    `json:"duration_seconds,omitempty"`
}

// RetentionPolicy represents the retention policy for jobs.
type RetentionPolicy struct {
	RecentJobs  string `json:"recent_jobs"`
	ByComponent string `json:"by_component"`
}

// ManifestExtended represents the manifest-extended.json structure.
type ManifestExtended struct {
	Comment                 string             `json:"_comment"`
	Purpose                 string             `json:"_purpose"`
	UpdateFrequency         string             `json:"_update_frequency"`
	ProjectID               string             `json:"project_id"`
	TwinVersion             int                `json:"twin_version"`
	FormatVersion           int                `json:"format_version"`
	Updated                 string             `json:"updated"`
	Statistics              *ProjectStatistics `json:"statistics"`
	Sources                 []*SourceInfo      `json:"sources"`
	TransformationPlatforms map[string]int     `json:"transformation_platforms"`
}

// ProjectStatistics represents project-level statistics.
type ProjectStatistics struct {
	TotalBuckets         int `json:"total_buckets"`
	TotalTables          int `json:"total_tables"`
	TotalTransformations int `json:"total_transformations"`
	TotalEdges           int `json:"total_edges"`
}

// SourceInfo represents information about a data source.
type SourceInfo struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Type        string   `json:"type"`
	Instances   int      `json:"instances"`
	TotalTables int      `json:"total_tables"`
	Buckets     []string `json:"buckets"`
}

// LineageEdge represents an edge in the lineage graph (JSONL format).
type LineageEdge struct {
	Source string `json:"source"`
	Target string `json:"target"`
	Type   string `json:"type"`
}

// LineageMeta represents metadata for the lineage graph.
type LineageMeta struct {
	Meta *LineageMetaData `json:"_meta"`
}

// LineageMetaData represents the metadata content.
type LineageMetaData struct {
	TotalEdges int    `json:"total_edges"`
	TotalNodes int    `json:"total_nodes"`
	Updated    string `json:"updated"`
}

// DocFields represents the standard documentation fields for JSON files.
type DocFields struct {
	Comment         string `json:"_comment"`
	Purpose         string `json:"_purpose"`
	UpdateFrequency string `json:"_update_frequency"`
	Security        string `json:"_security,omitempty"`
	Retention       string `json:"_retention,omitempty"`
}
