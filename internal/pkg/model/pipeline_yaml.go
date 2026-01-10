package model

// PipelineYAML represents the pipeline.yml structure for orchestrations.
type PipelineYAML struct {
	Version     int                     `yaml:"version" json:"version"`
	Name        string                  `yaml:"name,omitempty" json:"name,omitempty"`
	Description string                  `yaml:"description,omitempty" json:"description,omitempty"`
	Disabled    bool                    `yaml:"disabled,omitempty" json:"disabled,omitempty"`
	Tags        []string                `yaml:"tags,omitempty" json:"tags,omitempty"`
	Settings    *PipelineSettingsYAML   `yaml:"settings,omitempty" json:"settings,omitempty"`
	Phases      []PhaseYAML             `yaml:"phases" json:"phases"`
	Variables   map[string]VariableYAML `yaml:"variables,omitempty" json:"variables,omitempty"`
	Keboola     *KeboolaMetadata        `yaml:"_keboola,omitempty" json:"_keboola,omitempty"`
}

// KeboolaMetadata contains internal Keboola metadata managed by CLI.
type KeboolaMetadata struct {
	ComponentID string `yaml:"component_id" json:"component_id"`
	ConfigID    string `yaml:"config_id" json:"config_id"`
}

// PipelineSettingsYAML represents orchestration settings.
type PipelineSettingsYAML struct {
	Parallelism int    `yaml:"parallelism,omitempty" json:"parallelism,omitempty"`
	OnFailure   string `yaml:"on_failure,omitempty" json:"on_failure,omitempty"` // "stop" or "continue"
}

// PhaseYAML represents a phase in the orchestration.
type PhaseYAML struct {
	Name        string     `yaml:"name" json:"name"`
	Description string     `yaml:"description,omitempty" json:"description,omitempty"`
	Parallel    bool       `yaml:"parallel,omitempty" json:"parallel,omitempty"`
	DependsOn   []string   `yaml:"depends_on,omitempty" json:"depends_on,omitempty"`
	Tasks       []TaskYAML `yaml:"tasks" json:"tasks"`
}

// TaskYAML represents a task in a phase.
type TaskYAML struct {
	Name              string         `yaml:"name" json:"name"`
	Component         string         `yaml:"component" json:"component"`
	Config            string         `yaml:"config" json:"config"`
	Path              string         `yaml:"path,omitempty" json:"path,omitempty"` // Full path from project root
	Enabled           *bool          `yaml:"enabled,omitempty" json:"enabled,omitempty"`
	ContinueOnFailure bool           `yaml:"continue_on_failure,omitempty" json:"continue_on_failure,omitempty"`
	Parameters        map[string]any `yaml:"parameters,omitempty" json:"parameters,omitempty"`
}

// VariableYAML represents a variable definition in the pipeline.
type VariableYAML struct {
	Type        string `yaml:"type,omitempty" json:"type,omitempty"`
	Default     any    `yaml:"default,omitempty" json:"default,omitempty"`
	Description string `yaml:"description,omitempty" json:"description,omitempty"`
}

// SchedulesYAML represents the _schedules.yml structure.
type SchedulesYAML struct {
	Version   int            `yaml:"version" json:"version"`
	Schedules []ScheduleYAML `yaml:"schedules" json:"schedules"`
}

// ScheduleYAML represents a schedule definition.
type ScheduleYAML struct {
	Name        string         `yaml:"name" json:"name"`
	Description string         `yaml:"description,omitempty" json:"description,omitempty"`
	Cron        string         `yaml:"cron" json:"cron"`
	Timezone    string         `yaml:"timezone,omitempty" json:"timezone,omitempty"`
	Enabled     *bool          `yaml:"enabled,omitempty" json:"enabled,omitempty"`
	Variables   map[string]any `yaml:"variables,omitempty" json:"variables,omitempty"`
}
