package plan

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

type EncryptPlan struct {
	naming  *model.Naming
	actions []*EncryptAction
}

func (p *EncryptPlan) Name() string {
	return "encrypt"
}

func (p *EncryptPlan) Invoke(projectId int, logger *zap.SugaredLogger, encryptionApi *encryption.Api, projectState *state.State, ctx context.Context) error {
	return newEncryptExecutor(projectId, logger, encryptionApi, projectState, ctx, p).invoke()
}

func (p *EncryptPlan) Log(writer *log.WriteCloser) {
	writer.WriteStringNoErr(fmt.Sprintf(`Plan for "%s" operation:`, p.Name()))
	if len(p.actions) == 0 {
		writer.WriteStringNoErrIndent1("no values to encrypt")
	} else {
		for _, action := range p.actions {
			writer.WriteStringNoErrIndent1(action.manifest.Kind().Abbr + " " + action.manifest.Path())
			for _, value := range action.values {
				writer.WriteStringNoErrIndent(fmt.Sprintf("%v", value.path), 2)
			}
		}
	}
}

func (p *EncryptPlan) ValidateAllEncrypted() error {
	errors := utils.NewMultiError()
	for _, action := range p.actions {
		objectErrors := utils.NewMultiError()
		for _, value := range action.values {
			objectErrors.AppendRaw(value.path.String())
		}

		errors.AppendWithPrefix(
			fmt.Sprintf(
				`%s "%s" contains unencrypted values`,
				action.manifest.Kind().Name,
				p.naming.ConfigFilePath(action.manifest.Path()),
			),
			objectErrors,
		)
	}

	return errors.ErrorOrNil()
}
