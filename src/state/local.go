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
		// Validate, branch must be allowed
		if branch, ok := value.(*model.Branch); ok && !s.manifest.IsBranchAllowed(branch) {
			return nil, fmt.Errorf(
				`found manifest record for branch "%s" (%d), but it is not allowed by the manifest "allowedBranches"`,
				branch.Name,
				branch.Id,
			)
		}
		return s.SetLocalState(value, record), nil
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
