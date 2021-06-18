package diff

import "keboola-as-code/src/model"

type ModelState interface {
	diff() (Result, error)
}

type BranchState struct {
	*model.BranchState
}
type ConfigState struct {
	*model.ConfigState
}
type ConfigRowState struct {
	*model.ConfigRowState
}

func (b *BranchState) diff() (Result, error) {
	data := resultData{}
	return &BranchDiff{data, b}, nil
}

func (c *ConfigState) diff() (Result, error) {
	data := resultData{}
	return &ConfigDiff{data, c}, nil
}

func (r *ConfigRowState) diff() (Result, error) {
	data := resultData{}
	return &ConfigRowDiff{data, r}, nil
}
