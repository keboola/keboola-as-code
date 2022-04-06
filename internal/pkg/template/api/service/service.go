package service

import (
	"fmt"
	"sync"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/template/api/dependencies"
	. "github.com/keboola/keboola-as-code/internal/pkg/template/api/gen/templates"
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

func (s *service) IndexRoot(dependencies.Container) (err error) {
	// Redirect / -> /v1
	return nil
}

func (s *service) Index(dependencies.Container) (res *ServiceIndex, err error) {
	res = &ServiceIndex{
		API:           "templates",
		Documentation: "https://templates.keboola.com/v1/documentation",
	}
	return res, nil
}

func (s *service) HealthCheck(dependencies.Container) (res string, err error) {
	return "OK", nil
}

func (s *service) RepositoriesIndex(dependencies.Container, *RepositoriesIndexPayload) (res *Repositories, err error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *service) RepositoryIndex(dependencies.Container, *RepositoryIndexPayload) (res *Repository, err error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *service) TemplatesIndex(dependencies.Container, *TemplatesIndexPayload) (res *Templates, err error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *service) TemplateIndex(dependencies.Container, *TemplateIndexPayload) (res *Template, err error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *service) VersionIndex(dependencies.Container, *VersionIndexPayload) (res *TemplateVersion, err error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *service) Inputs(dependencies.Container, *InputsPayload) (res *InputsIndex, err error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *service) InputsValidate(dependencies.Container, *InputsValidatePayload) (res *ValidationResult, err error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *service) VersionUse(dependencies.Container, *VersionUsePayload) (res *UseTemplateResult, err error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *service) InstancesIndex(dependencies.Container, *InstancesIndexPayload) (err error) {
	return fmt.Errorf("not implemented")
}

func (s *service) InstanceIndex(dependencies.Container, *InstanceIndexPayload) (err error) {
	return fmt.Errorf("not implemented")
}

func (s *service) InstanceUpdate(dependencies.Container, *InstanceUpdatePayload) (err error) {
	return fmt.Errorf("not implemented")
}

func (s *service) InstanceDelete(dependencies.Container, *InstanceDeletePayload) (err error) {
	return fmt.Errorf("not implemented")
}

func (s *service) Upgrade(dependencies.Container, *UpgradePayload) (err error) {
	return fmt.Errorf("not implemented")
}

func (s *service) UpgradeInputs(dependencies.Container, *UpgradeInputsPayload) (err error) {
	return fmt.Errorf("not implemented")
}

func (s *service) UpgradeInputsValidate(dependencies.Container, *UpgradeInputsValidatePayload) (err error) {
	return fmt.Errorf("not implemented")
}
