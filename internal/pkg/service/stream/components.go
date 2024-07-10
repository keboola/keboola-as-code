package stream

import (
	"context"
	"sort"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/source/httpsource"
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
)

type Components []Component

type Component string

func StartComponents(ctx context.Context, d dependencies.ServiceScope, cfg config.Config, components ...Component) error {
	componentsMap := make(map[Component]bool)
	for _, c := range components {
		componentsMap[c] = true
	}

	// Start components, always in the same order
	if componentsMap[ComponentStorageCoordinator] {
		if err := coordinator.Start(ctx, d, cfg); err != nil {
			return err
		}
	}

	if componentsMap[ComponentStorageWriter] {
		if err := writernode.Start(ctx, d, cfg); err != nil {
			return err
		}
	}

	if componentsMap[ComponentStorageReader] {
		if err := readernode.Start(ctx, d, cfg); err != nil {
			return err
		}
	}

	if componentsMap[ComponentAPI] {
		if err := api.Start(ctx, d, cfg); err != nil {
			return err
		}
	}

	if componentsMap[ComponentHTTPSource] {
		d, err := dependencies.NewSourceScope(d, cfg)
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
