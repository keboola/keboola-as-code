package oauthproxy

import (
	"net/netip"
	"net/url"
	"strings"

	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/apis/options"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func validateProviderEndpoints(p options.Provider) error {
	endpoints := []struct {
		name  string
		value string
	}{
		{name: "issuer URL", value: p.OIDCConfig.IssuerURL},
		{name: "logout URL", value: p.BackendLogoutURL},
		{name: "login URL", value: p.LoginURL},
		{name: "redeem URL", value: p.RedeemURL},
		{name: "validate URL", value: p.ValidateURL},
		{name: "profile URL", value: p.ProfileURL},
	}

	errs := errors.NewMultiError()
	for _, endpoint := range endpoints {
		if endpoint.value == "" {
			continue
		}
		if err := validateEndpointURL(endpoint.value); err != nil {
			errs.Append(errors.PrefixErrorf(err, "invalid %s", endpoint.name))
		}
	}
	return errs.ErrorOrNil()
}

func validateEndpointURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return errors.Errorf(`"%s" is not a valid URL`, raw)
	}
	if u.Scheme != "https" {
		return errors.Errorf(`"%s" must use https`, raw)
	}

	host := strings.TrimSuffix(strings.ToLower(u.Hostname()), ".")
	if host == "" {
		return errors.Errorf(`"%s" must contain a host`, raw)
	}
	if _, err := netip.ParseAddr(host); err == nil {
		return errors.Errorf(`"%s" must use a host name, not an IP address`, raw)
	}
	if !strings.Contains(host, ".") || hasInternalSuffix(host) {
		return errors.Errorf(`"%s" must use a public host name`, raw)
	}

	return nil
}

func hasInternalSuffix(host string) bool {
	for _, suffix := range []string{".localhost", ".local", ".internal", ".svc", ".cluster.local"} {
		if strings.HasSuffix(host, suffix) {
			return true
		}
	}
	return false
}
