package definition

import "github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

const SinkTypeJobTrigger = SinkType("jobTrigger")

// JobTriggerSink configures a sink that triggers a Keboola Queue job on each received record.
// No local storage is used; a job is fired synchronously on WriteRecord.
type JobTriggerSink struct {
	// ComponentID is the component to run.
	ComponentID keboola.ComponentID `json:"componentId" validate:"required"`
	// ConfigID is the configuration of the component to run.
	ConfigID keboola.ConfigID `json:"configId" validate:"required"`
	// BranchID is the branch on which the job runs.
	BranchID keboola.BranchID `json:"branchId" validate:"required"`
	// ConfigDataTemplate is an optional Jsonnet template evaluated against the incoming request.
	// The template output (a JSON object) is passed as configData to the triggered job,
	// allowing webhook payload fields to override runtime parameters.
	// Available Jsonnet functions: Body(), Header(), Ip(), Now() — same as in table column templates.
	// If empty, the job runs with the component's default saved configuration.
	ConfigDataTemplate string `json:"configDataTemplate,omitempty"`
}
