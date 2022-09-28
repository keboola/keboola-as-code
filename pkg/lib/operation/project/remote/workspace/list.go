package workspace

import (
	"context"
	"fmt"
	"sync"

	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/sandboxesapi"
	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

type listDeps interface {
	Tracer() trace.Tracer
	Logger() log.Logger
	StorageApiClient() client.Sender
	SandboxesApiClient() client.Sender
}

func List(ctx context.Context, d listDeps) (err error) {
	ctx, span := d.Tracer().Start(ctx, "kac.lib.operation.project.remote.workspace.list")
	defer telemetry.EndSpan(span, &err)

	logger := d.Logger()

	branch, err := storageapi.GetDefaultBranchRequest().Send(ctx, d.StorageApiClient())
	if err != nil {
		return fmt.Errorf("cannot find default branch: %w", err)
	}

	logger.Info("Loading workspaces, please wait.")

	// Load configs and instances in parallel
	var configs []*storageapi.Config
	var instances map[string]*sandboxesapi.Sandbox
	errors := utils.NewMultiError()
	wg := &sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		data, err := sandboxesapi.ListConfigRequest(branch.ID).Send(ctx, d.StorageApiClient())
		if err != nil {
			errors.Append(fmt.Errorf("cannot list workspace configs: %w", err))
			return
		}
		configs = *data
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		data, err := sandboxesapi.ListRequest().Send(ctx, d.SandboxesApiClient())
		if err != nil {
			errors.Append(fmt.Errorf("cannot list workspaces: %w", err))
			return
		}
		m := make(map[string]*sandboxesapi.Sandbox, 0)
		for _, sandbox := range *data {
			m[sandbox.ID.String()] = sandbox
		}
		instances = m
	}()

	wg.Wait()

	logger.Info("Found workspaces:")
	for _, config := range configs {
		instanceId, err := sandboxesapi.GetSandboxID(config)
		if err != nil {
			logger.Debugf("  invalid workspace config (%s): %w", config.ID, err)
			continue
		}
		instance := instances[instanceId.String()]

		var info string
		if !sandboxesapi.SupportsSizes(instance.Type) {
			info = fmt.Sprintf("  ID: %s, Type: %s, Name: %s", instance.ID, instance.Type, config.Name)
		} else {
			info = fmt.Sprintf("  ID: %s, Type: %s, Size: %s, Name: %s", instance.ID, instance.Type, instance.Size, config.Name)
		}
		logger.Info(info)
	}

	return nil
}
