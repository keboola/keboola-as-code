package state

import (
	"fmt"
	"keboola-as-code/src/model"
	"keboola-as-code/src/utils"
)

// doLoadLocalState - manifest -> local files -> unified model
func (s *State) doLoadLocalState() {
	s.localErrors = utils.NewMultiError()

	// Branches
	for _, b := range s.manifest.Content.Branches {
		s.loadModel(b)
	}

	// Configs
	for _, c := range s.manifest.Content.Configs {
		s.loadModel(c.ConfigManifest)

		// Rows
		for _, r := range c.Rows {
			s.loadModel(r)
		}
	}
}

func (s *State) loadModel(record model.Record) model.ObjectState {
	// Detect record type
	var value model.Object
	switch v := record.(type) {
	case *model.BranchManifest:
		value = &model.Branch{BranchKey: v.BranchKey}
	case *model.ConfigManifest:
		value = &model.Config{ConfigKey: v.ConfigKey}
	case *model.ConfigRowManifest:
		value = &model.ConfigRow{ConfigRowKey: v.ConfigRowKey}
	default:
		panic(fmt.Errorf(`unexpected type %T`, record))
	}

	found, err := s.localManager.LoadModel(record, value)
	if err == nil {
		return s.SetLocalState(value, record)
	} else {
		record.State().SetInvalid()
		if !found {
			record.State().SetNotFound()
		}
		if found || !s.SkipNotFoundErr {
			s.AddLocalError(err)
		}
		return nil
	}
}
