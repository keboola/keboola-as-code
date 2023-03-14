package dependencies

import (
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	projectManifest "github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

func storageAPIHost(fs filesystem.Fs, opts *options.Options) (string, error) {
	var host string
	if fs.IsFile(projectManifest.Path()) {
		// Get host from manifest
		m, err := projectManifest.Load(fs, true)
		if err != nil {
			return "", err
		} else {
			host = m.APIHost()
		}
	} else {
		// Get host from options (ENV/flag)
		host = opts.GetString(options.StorageAPIHostOpt)
	}

	// Fallback
	if host == "" {
		host = "connection.keboola.com"
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
	return host, nil
}
