package dependencies

import (
	"context"
	"net/url"
	"strings"

	projectManifest "github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

// storageAPIHost loads the Storage API host by the following priority:
//
// 1. from the ".keboola/manifest.json file", if present.
//
// Or from the Options:
// 2. Flag.
// 3. ENV
// 4. An env file, e.g., ".env.local".
func storageAPIHost(ctx context.Context, baseScp BaseScope, fallback string, hostByFlag configmap.Value[string]) (string, error) {
	fs := baseScp.Fs()

	var host string
	if fs.IsFile(ctx, projectManifest.Path()) {
		// Get host from manifest
		m, err := projectManifest.Load(ctx, fs, true)
		if err != nil {
			return "", err
		} else {
			host = m.APIHost()
		}
	} else {
		// Get host from options (ENV/flag)
		host = hostByFlag.Value
		if hostByFlag.SetBy == configmap.SetByEnv {
			baseScp.Logger().Infof(ctx, `Storage API host "%s" set from ENV.`, host)
		}
	}

	// Fallback
	if host == "" {
		host = fallback
	}

	// Interactive dialog
	if host == "" {
		host = baseScp.Dialogs().AskStorageAPIHost()
	}

	// HTTP protocol can be explicitly specified in the host definition,
	// otherwise, HTTPS is used by default.
	useHTTP := strings.HasPrefix(host, "http://")

	// Normalize host and remove protocol
	if host = strhelper.NormalizeHost(host); host == "" {
		return "", ErrMissingStorageAPIHost
	}

	// Add protocol
	if useHTTP {
		host = "http://" + host
	} else {
		host = "https://" + host
	}

	if _, err := url.Parse(host); err != nil {
		return "", errors.Errorf(`invalid host "%s"`, host)
	}

	return host, nil
}
