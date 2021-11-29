package encrypt

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/encryption"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

type Plan struct {
	naming  *model.Naming
	actions []*action
}

func (p *Plan) Name() string {
	return "encrypt"
}

func (p *Plan) Invoke(projectId int, logger *zap.SugaredLogger, encryptionApi *encryption.Api, projectState *state.State, ctx context.Context) error {
	return newExecutor(projectId, logger, encryptionApi, projectState, ctx, p).invoke()
}

func (p *Plan) Log(writer *log.WriteCloser) {
	writer.WriteStringNoErr(fmt.Sprintf(`Plan for "%s" operation:`, p.Name()))
	if len(p.actions) == 0 {
		writer.WriteStringNoErrIndent1("no values to encrypt")
	} else {
		for _, action := range p.actions {
			writer.WriteStringNoErrIndent1(action.Kind().Abbr + " " + action.Path())
			for _, value := range action.values {
				writer.WriteStringNoErrIndent(fmt.Sprintf("%v", value.path), 2)
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
				p.naming.ConfigFilePath(action.Path()),
			),
			objectErrors,
		)
	}

	return errors.ErrorOrNil()
}
