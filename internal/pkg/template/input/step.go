package input

type StepsGroups []StepsGroup

type StepsGroup struct {
	Description string `json:"description" validate:"max=80"`
	Required    string `json:"required" validate:"oneof=all atLeastOne exactOne zeroOrOne optional"`
	Steps       []Step `json:"steps" validate:"min=1,dive"`
}

type Step struct {
	Icon              string `json:"icon" validate:"required"`
	Name              string `json:"name" validate:"required,max=20"`
	Description       string `json:"description" validate:"max=40"`
	DialogName        string `json:"dialogName,omitempty" validate:"omitempty,max=20"`
	DialogDescription string `json:"dialogDescription,omitempty" validate:"omitempty,max=200"`
	Inputs            Inputs `json:"inputs" validate:"omitempty,dive"`
}
