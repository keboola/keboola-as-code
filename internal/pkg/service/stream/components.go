package stream

import (
	"context"
	"sort"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	ComponentAPI = "api"
)

type Components map[Component]bool

type Component string

func StartComponents(ctx context.Context, d dependencies.ServiceScope, cfg config.Config, components map[Component]bool) error {
	// Start components, always in the same order
	if components[ComponentAPI] {
		if err := api.Start(ctx, d, cfg); err != nil {
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
	components := make(Components)
	var unexpected []string
	for _, component := range args {
		switch component {
		case ComponentAPI: // expected components
			components[Component(component)] = true
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
	names := make([]string, 0, len(v))
	for k := range v {
		names = append(names, string(k))
	}
	sort.Strings(names)
	return strings.Join(names, ",")
}
