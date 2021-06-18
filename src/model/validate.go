package model

import (
	"fmt"
	"keboola-as-code/src/utils"
	"keboola-as-code/src/validator"
)

type stateValidator struct {
	error *utils.Error
}

func validateState(state *State) *utils.Error {
	v := &stateValidator{}

	for _, b := range state.Branches() {
		v.validateBranch(b.Remote)
		v.validateBranch(b.Local)
	}

	for _, c := range state.Components() {
		v.validateComponent(c.Remote)
	}

	for _, c := range state.Configs() {
		v.validateConfig(c.Remote)
		v.validateConfig(c.Local)
	}

	for _, r := range state.ConfigRows() {
		v.validateConfigRow(r.Remote)
		v.validateConfigRow(r.Local)
	}

	return v.error
}

func (s *stateValidator) AddError(err error) {
	s.error.Add(err)
}

func (s *stateValidator) validateBranch(branch *Branch) {
	if err := validator.Validate(branch); err != nil {
		s.AddError(fmt.Errorf("branch is not valid: %s", err))
	}
}

func (s *stateValidator) validateComponent(component *Component) {
	if err := validator.Validate(component); err != nil {
		s.AddError(fmt.Errorf("component is not valid: %s", err))
	}
}

func (s *stateValidator) validateConfig(config *Config) {
	if err := validator.Validate(config); err != nil {
		s.AddError(fmt.Errorf("config is not valid: %s", err))
	}
}

func (s *stateValidator) validateConfigRow(configRow *ConfigRow) {
	if err := validator.Validate(configRow); err != nil {
		s.AddError(fmt.Errorf("config row is not valid: %s", err))
	}
}
