package model

const (
	MetaFileFieldsTag                 = "metaFile:true"        // marks meta fields in object struct
	ConfigFileFieldTag                = "configFile:true"      // marks config field in object struct
	DescriptionFileFieldTag           = "descriptionFile:true" // marks description field in object struct
	TransformationType                = "transformation"
	SharedCodeComponentId             = ComponentId("keboola.shared-code")
	OrchestratorComponentId           = ComponentId("keboola.orchestrator")
	ShareCodeTargetComponentKey       = `componentId`
	SharedCodeContentKey              = `code_content`
	VariablesIdContentKey             = `variables_id`
	VariablesValuesIdContentKey       = `variables_values_id`
	SharedCodeVariablesIdContentKey   = `variables_id`
	SharedCodeIdContentKey            = `shared_code_id`
	SharedCodeRowsIdContentKey        = `shared_code_row_ids`
	SharedCodePathContentKey          = `shared_code_path`
	SchedulerTargetKey                = `target`
	SchedulerTargetComponentIdKey     = `componentId`
	SchedulerTargetConfigurationIdKey = `configurationId`
	OrchestratorPhasesContentKey      = `phases`
	OrchestratorTasksContentKey       = `tasks`
)

// Ticket https://keboola.docs.apiary.io/#reference/tickets/generate-unique-id/generate-new-id
type Ticket struct {
	Id string `json:"id"`
}

// Token https://keboola.docs.apiary.io/#reference/tokens-and-permissions/token-verification/token-verification
type Token struct {
	Id       string     `json:"id"`
	Token    string     `json:"token"`
	IsMaster bool       `json:"isMasterToken"`
	Owner    TokenOwner `json:"owner"`
}

func (t *Token) ProjectId() int {
	return t.Owner.Id
}

func (t *Token) ProjectName() string {
	return t.Owner.Name
}

type TokenOwner struct {
	Id   int    `json:"id"`
	Name string `json:"name"`
}
