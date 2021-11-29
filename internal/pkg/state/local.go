package state

import (
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

// loadLocalState - manifest -> local files -> unified model.
func (s *State) loadLocalState() {
	s.localErrors = utils.NewMultiError()

	uow := s.localManager.NewUnitOfWork(s.context)
	if s.IgnoreNotFoundErr {
		uow.SkipNotFoundErr()
	}

	uow.LoadAll(s.manifest.Content)
	if err := uow.Invoke(); err != nil {
		s.AddLocalError(err)
	}
}
