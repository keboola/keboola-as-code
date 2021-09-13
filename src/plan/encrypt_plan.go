package plan

import (
	"fmt"
	"keboola-as-code/src/model"

	"go.uber.org/zap"

	"keboola-as-code/src/encryption"
	"keboola-as-code/src/log"
	"keboola-as-code/src/state"
	"keboola-as-code/src/utils"
)

type EncryptPlan struct {
	naming model.Naming
	actions []*EncryptAction
}

func (p *EncryptPlan) Name() string {
	return "encrypt"
}

func (p *EncryptPlan) Invoke(projectId int, logger *zap.SugaredLogger, encryptionApi *encryption.Api, projectState *state.State) error {
	return newEncryptExecutor(projectId, logger, encryptionApi, projectState, p).invoke()
}

func (p *EncryptPlan) Log(writer *log.WriteCloser) {
	writer.WriteStringNoErr(fmt.Sprintf(`Plan for "%s" operation:`, p.Name()))
	if len(p.actions) == 0 {
		writer.WriteStringNoErr("\tno values to encrypt")
	} else {
		for _, action := range p.actions {
			writer.WriteStringNoErr("\t" + action.manifest.Kind().Abbr + " " + action.manifest.RelativePath())
			for _, value := range action.values {
				writer.WriteStringNoErr(fmt.Sprintf("\t\t%v", value.path))
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
				p.naming.ConfigFilePath(action.manifest.RelativePath()),
			),
			objectErrors,
		)
	}

	return errors.ErrorOrNil()
}
