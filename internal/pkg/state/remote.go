package state

import (
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

// loadRemoteState - API -> unified model.
func (s *State) loadRemoteState() {
	s.remoteErrors = utils.NewMultiError()
	uow := s.remoteManager.NewUnitOfWork(s.context, "")
	uow.LoadAll()
	if err := uow.Invoke(); err != nil {
		s.AddRemoteError(err)
	}
}
