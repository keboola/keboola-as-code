package encrypt

import (
	"context"
	"fmt"

	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Plan struct {
	actions []*action
}

func (p *Plan) Empty() bool {
	return len(p.actions) == 0
}

func (p *Plan) Name() string {
	return "encrypt"
}

func (p *Plan) Invoke(ctx context.Context, projectID keboola.ProjectID, logger log.Logger, keboolaProjectAPI *keboola.API, state *state.State) error {
	return newExecutor(ctx, projectID, logger, keboolaProjectAPI, state, p).invoke()
}

func (p *Plan) Log(logger log.Logger) {
	writer := logger.InfoWriter()
	writer.WriteString(fmt.Sprintf(`Plan for "%s" operation:`, p.Name()))
	if len(p.actions) == 0 {
		writer.WriteStringIndent(1, "no values to encrypt")
	} else {
		for _, action := range p.actions {
			writer.WriteStringIndent(1, action.Kind().Abbr+" "+action.Path())
			for _, value := range action.values {
				writer.WriteStringIndent(2, fmt.Sprintf("%v", value.path))
			}
		}
	}
}

func (p *Plan) ValidateAllEncrypted() error {
	errs := errors.NewMultiError()
	for _, action := range p.actions {
		errs.Append(&UnencryptedValueError{
			kind:     action.Kind(),
			filePath: filesystem.Join(action.ObjectState.Manifest().Path(), naming.ConfigFile),
			values:   action.values,
		})
	}
	return errs.ErrorOrNil()
}

type UnencryptedValueError struct {
	kind     model.Kind
	filePath string
	values   []*UnencryptedValue
}

func (v UnencryptedValueError) Error() string {
	return fmt.Sprintf(`%s "%s" contains unencrypted values`, v.kind, v.filePath)
}

// WriteError writes formatted bullet list.
func (v UnencryptedValueError) WriteError(w errors.Writer, level int, trace errors.StackTrace) {
	w.WritePrefix(level, v.Error(), trace)
	w.WriteNewLine()

	for i, value := range v.values {
		if i != 0 {
			w.WriteNewLine()
		}
		w.WriteBullet(level + 1)
		w.Write(value.path.String())
	}
}
