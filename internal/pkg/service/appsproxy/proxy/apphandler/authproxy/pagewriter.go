package authproxy

import (
	"net/http"

	oauthproxy "github.com/oauth2-proxy/oauth2-proxy/v7"
	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/apis/options"
	proxypw "github.com/oauth2-proxy/oauth2-proxy/v7/pkg/app/pagewriter"
	"github.com/spf13/cast"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/auth/provider"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/pagewriter"
)

type proxyPageWriter proxypw.Writer

type pageWriter struct {
	proxyPageWriter
	app          api.AppConfig
	authProvider provider.Provider
	pageWriter   *pagewriter.Writer
}

func (m *Manager) newPageWriter(app api.AppConfig, authProvider provider.Provider, opts *options.Options) (*pageWriter, error) {
	parent, err := proxypw.NewWriter(
		proxypw.Opts{
			TemplatesPath:    opts.Templates.Path,
			CustomLogo:       opts.Templates.CustomLogo,
			ProxyPrefix:      opts.ProxyPrefix,
			Footer:           opts.Templates.Footer,
			Version:          oauthproxy.VERSION,
			Debug:            opts.Templates.Debug,
			ProviderName:     opts.Providers[0].Name,
			SignInMessage:    opts.Templates.Banner,
			DisplayLoginForm: opts.Templates.DisplayLoginForm,
		},
	)
	if err != nil {
		return nil, err
	}

	return &pageWriter{
		proxyPageWriter: parent,
		app:             app,
		authProvider:    authProvider,
		pageWriter:      m.pageWriter,
	}, nil
}

func (pw *pageWriter) WriteErrorPage(w http.ResponseWriter, req *http.Request, opts proxypw.ErrorPageOpts) {
	// Convert messages to string
	var messages []string
	for _, msg := range opts.Messages {
		if str := cast.ToString(msg); str != "" {
			messages = append(messages, str)
		}
	}

	if len(messages) == 0 {
		switch opts.Status {
		case http.StatusForbidden:
			messages = []string{"You do not have permission to access this resource."}
		case http.StatusUnauthorized:
			messages = []string{"You need to be logged in to access this resource."}
		case http.StatusInternalServerError:
			messages = []string{"Internal Server Error Oops! Something went wrong."}
		default:
			messages = []string{opts.AppError}
		}
	}

	pw.pageWriter.WriteErrorPage(w, req, opts.Status, messages, "")
}

func (pw *pageWriter) ProxyErrorHandler(w http.ResponseWriter, req *http.Request, err error) {
	pw.pageWriter.ProxyErrorHandler(w, req, err)
}

func (pw *pageWriter) WriteRobotsTxt(w http.ResponseWriter, req *http.Request) {
	pw.pageWriter.WriteRobotsTxt(w, req)
}
