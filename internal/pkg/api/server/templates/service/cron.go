package service

import (
	"context"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/api/server/templates/dependencies"
)

const TemplateRepositoriesPullInterval = 5 * time.Minute
const ComponentsUpdateInterval = 5 * time.Minute

func StartRepositoriesPullCron(ctx context.Context, d dependencies.ForServer) error {
	// Get dependencies
	manager := d.RepositoryManager()

	// Start background work
	go func() {
		d.Logger().Infof("repository pull cron ready")

		// Delay start to a rounded time
		interval := TemplateRepositoriesPullInterval
		startAt := time.Now().Truncate(interval).Add(interval)
		timer := time.NewTimer(time.Until(startAt))
		<-timer.C

		// Start ticker
		d.Logger().Infof("repository pull cron started at %s, interval=%s", time.Now().Format("15:04:05"), interval)
		ticker := time.NewTicker(interval)
		manager.Pull(ctx)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				manager.Pull(ctx)
			}
		}
	}()
	return nil
}

func StartComponentsCron(ctx context.Context, d dependencies.ForServer) error {
	// Get dependencies
	provider := d.ComponentsProvider()

	// Start background work
	go func() {
		d.Logger().Infof("components update cron ready")

		// Delay start to a rounded time
		interval := ComponentsUpdateInterval
		startAt := time.Now().Truncate(interval).Add(interval)
		timer := time.NewTimer(time.Until(startAt))
		<-timer.C

		// Start ticker
		d.Logger().Infof("components cron started at %s, interval=%s", time.Now().Format("15:04:05"), interval)
		ticker := time.NewTicker(interval)
		provider.UpdateAsync(ctx)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				provider.UpdateAsync(ctx)
			}
		}
	}()
	return nil
}
