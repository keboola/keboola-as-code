package dependencies

import "github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"

// storageAPIToken loads the Storage API token from the Options by the following priority:
// 1. Flag.
// 2. ENV
// 3. An env file, e.g., ".env.local".
func storageAPIToken(baseScp BaseScope, tokenByFlags configmap.Value[string]) (string, error) {
	// Get token from options (ENV/flag)
	token := tokenByFlags.Value

	// Interactive dialog
	if token == "" {
		token = baseScp.Dialogs().AskStorageAPIToken()
	}

	// Validate
	if token == "" {
		return "", ErrMissingStorageAPIToken
	}

	return token, nil
}
