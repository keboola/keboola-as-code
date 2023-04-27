package validateconfig

import (
	"bytes"
	"context"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/go-utils/pkg/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Options struct {
	ComponentID keboola.ComponentID
	ConfigPath  string
}

type dependencies interface {
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
	Fs() filesystem.Fs
	Components() *model.ComponentsMap
}

func Run(ctx context.Context, o Options, d dependencies) (err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "kac.lib.operation.project.local.validate.config")
	defer telemetry.EndSpan(span, &err)
	logger := d.Logger()

	// Get component
	component, err := d.Components().GetOrErr(o.ComponentID)
	if err != nil {
		return err
	}

	// Read file
	fs := d.Fs()
	f, err := fs.FileLoader().ReadJSONFile(filesystem.NewFileDef(filesystem.Join(fs.WorkingDir(), o.ConfigPath)))
	if err != nil {
		return err
	}

	// File cannot be empty
	if v, ok := f.Content.GetOrNil("parameters").(*orderedmap.OrderedMap); !ok || len(v.Keys()) == 0 {
		return errors.Errorf("configuration is empty")
	}

	// Validate
	if len(component.Schema) == 0 || bytes.Equal(component.Schema, []byte("{}")) {
		logger.Warnf(`Component "%s" has no configuration JSON schema.`, component.ID)
	} else if err := schema.ValidateContent(component.Schema, f.Content); err != nil {
		return err
	}

	logger.Info("Validation done.")
	return nil
}
