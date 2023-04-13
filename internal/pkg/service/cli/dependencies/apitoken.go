package dependencies

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/options"
)

func storageAPIToken(d Base) (string, error) {
	// Get token from options (ENV/flag)
	token := d.Options().GetString(options.StorageAPITokenOpt)

	// Interactive dialog
	if token == "" {
		token = d.Dialogs().AskStorageAPIToken()
	}

	// Validate
	if token == "" {
		return "", ErrMissingStorageAPIToken
	}

	return token, nil
}
