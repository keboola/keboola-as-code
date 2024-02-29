package dependencies

// storageAPIToken loads the Storage API token from the Options by the following priority:
// 1. Flag.
// 2. ENV
// 3. An env file, e.g., ".env.local".
func storageAPIToken(baseScp BaseScope) (string, error) {
	// Get token from options (ENV/flag)
	token := baseScp.GlobalFlags().StorageAPIToken

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
