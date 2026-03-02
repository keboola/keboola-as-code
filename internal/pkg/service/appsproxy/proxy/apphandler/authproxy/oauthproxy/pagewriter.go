package oauthproxy

import (
	"net/http"
	"strings"

	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/apis/options"
	proxypw "github.com/oauth2-proxy/oauth2-proxy/v7/pkg/app/pagewriter"
	oauthproxy "github.com/oauth2-proxy/oauth2-proxy/v7/pkg/version"
	"github.com/spf13/cast"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/auth/provider"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/pagewriter"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
)

type proxyPageWriter proxypw.Writer

// pageWriter is adapter between common page writer and OAuth2Proxy specific page writer.
type pageWriter struct {
	proxyPageWriter
	logger       log.Logger
	app          api.AppConfig
	authProvider provider.Provider
	pageWriter   *pagewriter.Writer
}

func newPageWriter(logger log.Logger, pw *pagewriter.Writer, app api.AppConfig, authProvider provider.Provider, opts *options.Options) (*pageWriter, error) {
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
		logger:          logger.WithComponent("oauth2proxy.pw"),
		proxyPageWriter: parent,
		app:             app,
		authProvider:    authProvider,
		pageWriter:      pw,
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

	// Default messages
	if len(messages) == 0 {
		switch opts.Status {
		case http.StatusUnauthorized:
			messages = []string{"You need to be logged in to access this resource."}
		case http.StatusForbidden:
			messages = []string{"You do not have permission to access this resource."}
		case http.StatusInternalServerError:
			messages = []string{"Oops! Something went wrong."}
		default:
			messages = []string{opts.AppError}
		}
	}

	// Exception ID
	exceptionID := pagewriter.ExceptionIDPrefix + opts.RequestID

	joinedMessage := strings.Join(messages, "\n")
	// Add attributes
	req = req.WithContext(ctxattr.ContextWith(
		req.Context(),
		semconv.HTTPStatusCode(opts.Status),
		attribute.String("exceptionId", exceptionID),
		attribute.String("error.userMessages", joinedMessage),
		attribute.String("error.details", opts.AppError),
	))

	// Log warning
	pw.logger.Warn(req.Context(), strings.Join(messages, "\n")) //nolint:contextcheck // false positive

	pw.pageWriter.WriteErrorPage(w, req, &pw.app, opts.Status, joinedMessage+"\n"+opts.AppError, exceptionID)
}

func (pw *pageWriter) ProxyErrorHandler(w http.ResponseWriter, req *http.Request, err error) {
	pw.pageWriter.ProxyErrorHandler(w, req, pw.app, err)
}

func (pw *pageWriter) WriteRobotsTxt(w http.ResponseWriter, req *http.Request) {
	pw.pageWriter.WriteRobotsTxt(w, req)
}
