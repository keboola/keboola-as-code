package state

import (
	"fmt"
	"keboola-as-code/src/manifest"
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

func (s *State) loadModel(record manifest.Record) ObjectState {
	// Detect record type
	var value interface{}
	switch v := record.(type) {
	case *manifest.BranchManifest:
		value = &model.Branch{BranchKey: v.BranchKey}
	case *manifest.ConfigManifest:
		value = &model.Config{ConfigKey: v.ConfigKey}
	case *manifest.ConfigRowManifest:
		value = &model.ConfigRow{ConfigRowKey: v.ConfigRowKey}
	default:
		panic(fmt.Errorf(`unexpected type %T`, record))
	}

	found, err := s.localManager.LoadModel(record, value)
	if err == nil {
		switch v := value.(type) {
		case *model.Branch:
			return s.SetBranchLocalState(v, record.(*manifest.BranchManifest))
		case *model.Config:
			return s.SetConfigLocalState(v, record.(*manifest.ConfigManifest))
		case *model.ConfigRow:
			return s.SetConfigRowLocalState(v, record.(*manifest.ConfigRowManifest))
		default:
			panic(fmt.Errorf(`unexpected type %T`, record))
		}
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
