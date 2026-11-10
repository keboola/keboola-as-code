package k8sapp

import (
	"net"
	"net/url"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
)

// PublicOrigin is the scheme and host at which a browser reaches the workload.
// A Sandbox is reached only at the host it published; the app config names the App's host.
func PublicOrigin(publicURL *url.URL, app api.AppConfig, workload WorkloadRef, info AppInfo, known bool) (string, bool) {
	host := app.BaseURL(publicURL).Host
	if workload.IsSandbox() {
		if !known || info.PublicHost == "" {
			return "", false
		}
		host = info.PublicHost
		if port := publicURL.Port(); port != "" {
			host = net.JoinHostPort(host, port)
		}
	}
	return publicURL.Scheme + "://" + host, true
}
