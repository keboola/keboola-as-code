package encrypt

import (
	"context"
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/encryption"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

type Plan struct {
	*state.State
	actions []*action
}

func (p *Plan) Empty() bool {
	return len(p.actions) == 0
}

func (p *Plan) Name() string {
	return "encrypt"
}

func (p *Plan) Invoke(logger log.Logger, encryptionApi *encryption.Api, ctx context.Context) error {
	return newExecutor(logger, encryptionApi, ctx, p).invoke()
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
	errors := utils.NewMultiError()
	for _, action := range p.actions {
		objectErrors := utils.NewMultiError()
		for _, value := range action.values {
			objectErrors.Append(fmt.Errorf(value.path.String()))
		}

		errors.AppendWithPrefix(
			fmt.Sprintf(
				`%s "%s" contains unencrypted values`,
				action.Kind().Name,
				p.NamingGenerator().ConfigFilePath(action.Path()),
			),
			objectErrors,
		)
	}

	return errors.ErrorOrNil()
}
