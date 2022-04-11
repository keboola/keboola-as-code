package service

import (
	"sync"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/api/server/templates/dependencies"
	. "github.com/keboola/keboola-as-code/internal/pkg/api/server/templates/gen/templates"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
)

type service struct {
	dependencies        dependencies.Container
	lock                *sync.Mutex
	repositoriesManager *repository.Manager
}

const TemplateRepositoriesPullInterval = 5 * time.Minute

func New(d dependencies.Container) (Service, error) {
	repoManager, err := repository.NewManager(d.Logger())
	if err != nil {
		return nil, err
	}
	s := &service{
		dependencies:        d,
		lock:                &sync.Mutex{},
		repositoriesManager: repoManager,
	}
	s.StartCron()
	return s, nil
}

func (s *service) StartCron() {
	go func() {
		interval := TemplateRepositoriesPullInterval

		// Delay start to a rounded time
		startAt := time.Now().Truncate(interval).Add(interval)
		timer := time.NewTimer(time.Until(startAt))
		<-timer.C
		s.dependencies.Logger().Info("pull ticker started")

		ticker := time.NewTicker(interval)
		for {
			select {
			case <-s.dependencies.Ctx().Done():
				return
			case <-ticker.C:
				s.pullTemplateRepositories()
			}
		}
	}()
}

func (s *service) pullTemplateRepositories() {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.repositoriesManager.Pull()
}

func (s *service) APIRootIndex(dependencies.Container) (err error) {
	// Redirect / -> /v1
	return nil
}

func (s *service) APIVersionIndex(dependencies.Container) (res *ServiceDetail, err error) {
	res = &ServiceDetail{
		API:           "templates",
		Documentation: "https://templates.keboola.com/v1/documentation",
	}
	return res, nil
}

func (s *service) HealthCheck(dependencies.Container) (res string, err error) {
	return "OK", nil
}

func (s *service) RepositoriesIndex(dependencies.Container, *RepositoriesIndexPayload) (res *Repositories, err error) {
	return nil, NotImplementedError{}
}

func (s *service) RepositoryIndex(dependencies.Container, *RepositoryIndexPayload) (res *Repository, err error) {
	return nil, NotImplementedError{}
}

func (s *service) TemplatesIndex(dependencies.Container, *TemplatesIndexPayload) (res *Templates, err error) {
	return nil, NotImplementedError{}
}

func (s *service) TemplateIndex(dependencies.Container, *TemplateIndexPayload) (res *TemplateDetail, err error) {
	return nil, NotImplementedError{}
}

func (s *service) VersionIndex(dependencies.Container, *VersionIndexPayload) (res *TemplateVersionDetail, err error) {
	return nil, NotImplementedError{}
}

func (s *service) InputsIndex(dependencies.Container, *InputsIndexPayload) (res *Inputs, err error) {
	return nil, NotImplementedError{}
}

func (s *service) ValidateInputs(dependencies.Container, *ValidateInputsPayload) (res *ValidationResult, err error) {
	return nil, NotImplementedError{}
}

func (s *service) UseTemplateVersion(dependencies.Container, *UseTemplateVersionPayload) (res *UseTemplateDetail, err error) {
	return nil, NotImplementedError{}
}

func (s *service) InstancesIndex(dependencies.Container, *InstancesIndexPayload) (err error) {
	return NotImplementedError{}
}

func (s *service) InstanceIndex(dependencies.Container, *InstanceIndexPayload) (err error) {
	return NotImplementedError{}
}

func (s *service) UpdateInstance(dependencies.Container, *UpdateInstancePayload) (err error) {
	return NotImplementedError{}
}

func (s *service) DeleteInstance(dependencies.Container, *DeleteInstancePayload) (err error) {
	return NotImplementedError{}
}

func (s *service) UpgradeInstance(dependencies.Container, *UpgradeInstancePayload) (err error) {
	return NotImplementedError{}
}

func (s *service) UpgradeInstanceInputsIndex(dependencies.Container, *UpgradeInstanceInputsIndexPayload) (err error) {
	return NotImplementedError{}
}

func (s *service) UpgradeInstanceValidateInputs(dependencies.Container, *UpgradeInstanceValidateInputsPayload) (err error) {
	return NotImplementedError{}
}
