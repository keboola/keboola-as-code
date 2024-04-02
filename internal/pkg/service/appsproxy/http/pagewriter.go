package http

import (
	"net"
	"net/http"

	oauthproxy "github.com/oauth2-proxy/oauth2-proxy/v7"
	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/apis/options"
	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/app/pagewriter"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type parentWriter pagewriter.Writer

type pageWriter struct {
	parentWriter
	dnsErrorHandler func(w http.ResponseWriter, req *http.Request)
}

func NewPageWriter(opts *options.Options, dnsErrorHandler func(w http.ResponseWriter, req *http.Request)) (pagewriter.Writer, error) {
	parentWriter, err := pagewriter.NewWriter(
		pagewriter.Opts{
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

	return &pageWriter{parentWriter: parentWriter, dnsErrorHandler: dnsErrorHandler}, nil
}

func (pw *pageWriter) ProxyErrorHandler(w http.ResponseWriter, req *http.Request, err error) {
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) && dnsErr.IsNotFound {
		pw.dnsErrorHandler(w, req)
		return
	}

	pw.parentWriter.ProxyErrorHandler(w, req, err)
}
