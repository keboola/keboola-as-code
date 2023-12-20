package service

import (
	"context"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/templates/dependencies"
)

const (
	TemplateRepositoriesPullInterval = 5 * time.Minute
	ComponentsUpdateInterval         = 5 * time.Minute
)

func StartRepositoriesPullCron(ctx context.Context, d dependencies.APIScope) error {
	// Get dependencies
	manager := d.RepositoryManager()

	// Start background work
	d.Logger().InfofCtx(ctx, "repository pull cron ready")
	go func() {
		// Delay start to a rounded time
		interval := TemplateRepositoriesPullInterval
		startAt := time.Now().Truncate(interval).Add(interval)
		timer := time.NewTimer(time.Until(startAt))
		<-timer.C

		// Start ticker
		d.Logger().InfofCtx(ctx, "repository pull cron started at %s, interval=%s", time.Now().Format("15:04:05"), interval)
		ticker := time.NewTicker(interval)
		manager.Update(ctx)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				manager.Update(ctx)
			}
		}
	}()
	return nil
}

func StartComponentsCron(ctx context.Context, d dependencies.APIScope) error {
	// Get dependencies
	provider := d.ComponentsProvider()

	// Start background work
	d.Logger().InfofCtx(ctx, "components update cron ready")
	go func() {
		// Delay start to a rounded time
		interval := ComponentsUpdateInterval
		startAt := time.Now().Truncate(interval).Add(interval)
		timer := time.NewTimer(time.Until(startAt))
		<-timer.C

		// Start ticker
		d.Logger().InfofCtx(ctx, "components cron started at %s, interval=%s", time.Now().Format("15:04:05"), interval)
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
