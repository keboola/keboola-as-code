package stream

import (
	"context"
	"sort"
	"strings"

	commonDeps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/source/type/httpsource"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/node/coordinator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/node/readernode"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/node/writernode"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	ComponentAPI                Component = "api"
	ComponentHTTPSource         Component = "http-source"
	ComponentStorageCoordinator Component = "storage-coordinator"
	ComponentStorageWriter      Component = "storage-writer"
	ComponentStorageReader      Component = "storage-reader"
	ExceptionIDPrefix                     = "keboola-stream-"
)

type Components []Component

type Component string

func StartComponents(ctx context.Context, serviceScp dependencies.ServiceScope, cfg config.Config, components ...Component) (err error) {
	componentsMap := make(map[Component]bool)
	for _, c := range components {
		componentsMap[c] = true
	}

	// Common task scope
	var taskScp commonDeps.TaskScope
	if componentsMap[ComponentAPI] {
		taskScp, err = commonDeps.NewTaskScope(ctx, cfg.NodeID, ExceptionIDPrefix, serviceScp)
		if err != nil {
			return err
		}
	}

	// Common distribution scope
	var distScp commonDeps.DistributionScope
	if componentsMap[ComponentAPI] || componentsMap[ComponentStorageWriter] || componentsMap[ComponentHTTPSource] || componentsMap[ComponentStorageCoordinator] {
		distScp = commonDeps.NewDistributionScope(cfg.NodeID, cfg.Distribution, serviceScp)
	}

	// Common storage scope
	var storageScp dependencies.StorageScope
	if componentsMap[ComponentStorageWriter] || componentsMap[ComponentStorageReader] {
		storageScp, err = dependencies.NewStorageScope(ctx, serviceScp, cfg)
		if err != nil {
			return err
		}
	}

	// Start components, always in the same order
	if componentsMap[ComponentStorageCoordinator] {
		d, err := dependencies.NewCoordinatorScope(ctx, serviceScp, distScp, cfg)
		if err != nil {
			return err
		}
		if err := coordinator.Start(ctx, d, cfg); err != nil {
			return err
		}
	}

	if componentsMap[ComponentStorageWriter] {
		d, err := dependencies.NewStorageWriterScope(ctx, storageScp, distScp, cfg)
		if err != nil {
			return err
		}
		if err := writernode.Start(ctx, d, cfg); err != nil {
			return err
		}
	}

	if componentsMap[ComponentStorageReader] {
		d, err := dependencies.NewStorageReaderScope(ctx, storageScp, cfg)
		if err != nil {
			return err
		}
		if err := readernode.Start(ctx, d, cfg); err != nil {
			return err
		}
	}

	if componentsMap[ComponentAPI] {
		apiScp, err := dependencies.NewAPIScope(serviceScp, distScp, taskScp, cfg) // nolint:forbidigo
		if err != nil {
			return err
		}
		if err := api.Start(ctx, apiScp, cfg); err != nil {
			return err
		}
	}

	if componentsMap[ComponentHTTPSource] {
		d, err := dependencies.NewSourceScope(serviceScp, distScp, "http-source", cfg)
		if err != nil {
			return err
		}
		if err := httpsource.Start(ctx, d, cfg.Source.HTTP); err != nil {
			return err
		}
	}

	return nil
}

func ParseComponentsList(args []string) (Components, error) {
	// Skip binary name
	if len(args) > 0 {
		args = args[1:]
	}

	// At least one component must be enabled
	if len(args) == 0 {
		return nil, errors.Errorf("specify at least one service component as a positional argument")
	}

	// Create map of enabled components
	var components Components
	var unexpected []string
	for _, component := range args {
		switch Component(component) {
		// expected components
		case ComponentAPI, ComponentHTTPSource, ComponentStorageCoordinator, ComponentStorageWriter, ComponentStorageReader:
			components = append(components, Component(component))
		default:
			unexpected = append(unexpected, component)
		}
	}

	// Stop if an unexpected component is found
	if len(unexpected) > 0 {
		return nil, errors.Errorf(`unexpected service component: "%s"`, strings.Join(unexpected, `", "`))
	}

	return components, nil
}

func (v Components) String() string {
	var names []string
	for _, n := range v {
		names = append(names, string(n))
	}
	sort.Strings(names)
	return strings.Join(names, ",")
}
