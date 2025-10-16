// Package manager coordinates loading and caching of template repositories and templates.
//
// How it works:
//   - Manager is created by New function.
//   - Default repositories are preloaded in New by Manager.Repository method.
//   - Manager.Repository creates a new CachedRepository by newCachedRepository function.
//   - newCachedRepository function preloads all templates by CachedRepository.loadAllTemplates method.
//   - Manager.Repository calls CachedRepository.lock method for every request.
//   - After the request is finished, it must call provided UnlockFn.
//   - Manager.Update is called periodically, it calls CachedRepository.update.
//   - If there has been a change in the underlying git repository, then CachedRepository.update will return an updated copy of the repository.
//   - So there EXIST BOTH a new and an old version at the same time.
//   - Older requests will finish with the old repository version, new ones will use the new version.
//   - When all old requests are completed, freeLock is released, so the free method/goroutine is unblocked.
//   - So the repository and underlying FS are cleaned.
package manager

import (
	"context"
	"sort"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"golang.org/x/sync/singleflight"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/git"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	checkoutOp "github.com/keboola/keboola-as-code/pkg/lib/operation/repository/checkout"
	loadRepositoryOp "github.com/keboola/keboola-as-code/pkg/lib/operation/template/repository/load"
)

// Manager provides CachedRepository and templates for Templates API requests.
// It also contains list of the default repositories.
type Manager struct {
	ctx       context.Context
	deps      dependencies
	logger    log.Logger
	syncMeter metric.Float64Histogram

	// List of default repositories for the stack/server.
	// This list is individually modified in the Service according to the set project features.
	defaultRepositories []model.TemplateRepository

	repositories     map[string]*CachedRepository
	repositoriesInit *singleflight.Group // each template is load only once
	repositoriesLock *sync.RWMutex       // provides atomic access to the repositories field
}

type dependencies interface {
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
	Process() *servicectx.Process
	Components() *model.ComponentsMap
}

func New(ctx context.Context, d dependencies, defaultRepositories []model.TemplateRepository) (*Manager, error) {
	m := &Manager{
		ctx:                 ctx,
		deps:                d,
		logger:              d.Logger(),
		syncMeter:           d.Telemetry().Meter().FloatHistogram("keboola.go.templates.repo.sync.duration", "Templates repository sync duration.", "ms"),
		defaultRepositories: defaultRepositories,
		repositories:        make(map[string]*CachedRepository),
		repositoriesInit:    &singleflight.Group{},
		repositoriesLock:    &sync.RWMutex{},
	}

	// Free all repositories on server shutdown
	d.Process().OnShutdown(func(ctx context.Context) {
		m.Free(ctx)
	})

	// Init default repositories in parallel.
	// It preloads all default repositories and templates in them.
	errs := errors.NewMultiError()
	initWg := &sync.WaitGroup{}
	for _, repo := range defaultRepositories {
		initWg.Go(func() {
			if _, err := m.repository(ctx, repo); err != nil {
				errs.Append(err)
			}
		})
	}
	initWg.Wait()
	return m, errs.ErrorOrNil()
}

// DefaultRepositories returns list of default repositories configured for the API.
func (m *Manager) DefaultRepositories() []model.TemplateRepository {
	return m.defaultRepositories
}

// ManagedRepositories return list of git repositories which are updated when the Pull method is called.
func (m *Manager) ManagedRepositories() (out []string) {
	m.repositoriesLock.RLock()
	defer m.repositoriesLock.RUnlock()
	for _, r := range m.repositories {
		out = append(out, r.String())
	}
	sort.Strings(out)
	return out
}

func (m *Manager) Repository(ctx context.Context, ref model.TemplateRepository) (*CachedRepository, UnlockFn, error) {
	cachedRepo, err := m.repository(ctx, ref)
	if err != nil {
		return nil, nil, err
	}

	// Prevented cleaning of the repository if it is outdated but still in use by some API request.
	// UnlockFn must be called when the repository is no longer used.
	unlockFn := cachedRepo.lock()
	return cachedRepo, unlockFn, nil
}

func (m *Manager) Update(ctx context.Context) <-chan error {
	errorCh := make(chan error, 1)
	errs := errors.NewMultiError()

	// Lock repositories field (during reading it in following for cycle)
	m.repositoriesLock.Lock()

	// Pull repositories in parallel
	wait := &sync.WaitGroup{}
	for _, repo := range m.repositories {
		repoDef := repo.Unwrap().Definition()
		oldValue := repo
		wait.Go(func() {

			// Update
			startTime := time.Now()
			newValue, changed, err := oldValue.update(ctx)

			// Metric
			elapsedTime := float64(time.Since(startTime)) / float64(time.Millisecond)
			m.syncMeter.Record(ctx, elapsedTime, metric.WithAttributes(
				attribute.String("repo.name", repoDef.Name),
				attribute.String("repo.url", repoDef.URL),
				attribute.String("repo.ref", repoDef.Ref),
				attribute.String("error_type", telemetry.ErrorType(err)),
				attribute.Bool("is_init", false),
				attribute.Bool("is_success", err == nil),
				attribute.Bool("is_changed", changed),
			))

			// Handle error
			if err != nil {
				errs.Append(err)
				return
			}

			// Replace value
			if changed {
				m.repositoriesLock.Lock()
				m.repositories[newValue.Hash()] = newValue
				m.repositoriesLock.Unlock()

				// Free previous value
				oldValue.free(ctx)
			}
		})
	}

	// Unlock repositories field for updates
	m.repositoriesLock.Unlock()

	// Write error to channel if any
	go func() {
		wait.Wait()
		errorCh <- errs.ErrorOrNil()
	}()

	return errorCh
}

func (m *Manager) Free(ctx context.Context) {
	m.repositoriesLock.RLock()
	defer m.repositoriesLock.RUnlock()

	wg := &sync.WaitGroup{}
	for _, repo := range m.repositories {
		wg.Go(func() {
			<-repo.free(ctx)
		})
	}

	wg.Wait()
	m.logger.Infof(ctx, "repository manager cleaned up")
}

func (m *Manager) repository(ctx context.Context, ref model.TemplateRepository) (*CachedRepository, error) {
	hash := ref.Hash()

	// Check if is repository already loaded
	m.repositoriesLock.RLock()
	value, found := m.repositories[hash] // nolint
	m.repositoriesLock.RUnlock()
	if found {
		return value, nil
	}

	// Load repository, there is used "single flight" library:
	// the function is called only once, but every caller will get the same results.
	ch := m.repositoriesInit.DoChan(hash, func() (out any, err error) {
		// Metric
		startTime := time.Now()
		defer func() {
			elapsedTime := float64(time.Since(startTime)) / float64(time.Millisecond)
			m.syncMeter.Record(ctx, elapsedTime, metric.WithAttributes(
				attribute.String("repo.name", ref.Name),
				attribute.String("repo.url", ref.URL),
				attribute.String("repo.ref", ref.Ref),
				attribute.String("error_type", telemetry.ErrorType(err)),
				attribute.Bool("is_init", true),
				attribute.Bool("is_success", err == nil),
				attribute.Bool("is_changed", true),
			))
		}()

		// Load git repository
		var gitRepo git.Repository
		if ref.Type == model.RepositoryTypeGit {
			// Remote repository
			startTime := time.Now()
			m.logger.Infof(ctx, `checking out repository "%s:%s"`, ref.URL, ref.Ref)

			// Checkout
			gitRepo, err = checkoutOp.Run(ctx, ref, m.deps)
			if err != nil {
				return nil, err
			}

			// Checkout done
			m.logger.WithDuration(time.Since(startTime)).Infof(ctx, `checked out repository "%s"`, gitRepo)
		} else {
			// Local directory
			fs, err := aferofs.NewLocalFs(ref.URL, filesystem.WithLogger(m.deps.Logger()))
			if err != nil {
				return nil, err
			}
			gitRepo = git.NewLocalRepository(ref, fs)
			m.logger.Infof(ctx, `found local repository "%s"`, gitRepo)
		}

		// Load content of the template repository
		fs, unlockFn := gitRepo.Fs()
		data, err := loadRepositoryOp.Run(ctx, m.deps, ref, loadRepositoryOp.WithFs(fs))
		// Handle error
		if err != nil {
			unlockFn()
			return nil, err
		}

		// Initialize cached repository, preload all templates
		r := newCachedRepository(ctx, m.deps, gitRepo, unlockFn, data)

		// Cache value
		m.repositoriesLock.Lock()
		defer m.repositoriesLock.Unlock()
		m.repositories[hash] = r
		return r, nil
	})

	// Check result
	result := <-ch
	if err := result.Err; err != nil {
		return nil, err
	}
	return result.Val.(*CachedRepository), nil
}
