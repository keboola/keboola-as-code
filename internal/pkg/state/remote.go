package state

import (
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

// doLoadRemoteState - API -> unified model.
func (s *State) doLoadRemoteState() {
	s.remoteErrors = utils.NewMultiError()
	uow := s.remoteManager.NewUnitOfWork(s.context, "")
	uow.LoadAllTo(s)
	if err := uow.Invoke(); err != nil {
		s.remoteErrors.Append(err)
	}
}
