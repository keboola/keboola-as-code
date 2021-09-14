package state

import (
	"fmt"

	"keboola-as-code/src/model"
	"keboola-as-code/src/utils"
)

// doLoadLocalState - manifest -> local files -> unified model.
func (s *State) doLoadLocalState() {
	s.localErrors = utils.NewMultiError()

	// Branches
	for _, b := range s.manifest.Content.Branches {
		if _, err := s.LoadModel(b); err != nil {
			s.AddLocalError(err)
		}
	}

	// Configs
	for _, c := range s.manifest.Content.Configs {
		if _, err := s.LoadModel(c.ConfigManifest); err != nil {
			s.AddLocalError(err)
		}

		// Rows
		for _, r := range c.Rows {
			if _, err := s.LoadModel(r); err != nil {
				s.AddLocalError(err)
			}
		}
	}
}

func (s *State) LoadModel(record model.Record) (model.ObjectState, error) {
	// Detect record type
	var object model.Object
	switch v := record.(type) {
	case *model.BranchManifest:
		object = &model.Branch{BranchKey: v.BranchKey}
	case *model.ConfigManifest:
		object = &model.Config{ConfigKey: v.ConfigKey}
	case *model.ConfigRowManifest:
		object = &model.ConfigRow{ConfigRowKey: v.ConfigRowKey}
	default:
		panic(fmt.Errorf(`unexpected type %T`, record))
	}

	found, err := s.localManager.LoadModel(record, object)
	if err == nil {
		// Validate, branch must be allowed
		if s.manifest.IsObjectIgnored(object) {
			return nil, fmt.Errorf(
				`found manifest record for %s "%s", but it is not allowed by the manifest definition`,
				object.Kind().Name,
				object.ObjectId(),
			)
		}
		return s.SetLocalState(object, record), nil
	} else {
		record.State().SetInvalid()
		if !found {
			record.State().SetNotFound()
		}
		if found || !s.SkipNotFoundErr {
			return nil, err
		}
		return nil, nil
	}
}
