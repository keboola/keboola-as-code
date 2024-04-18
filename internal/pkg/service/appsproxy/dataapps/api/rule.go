package api

import (
	"net/http"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/auth/provider"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	// RulePathPrefix registers the Rule.Value as http.ServeMux pattern.
	//
	// Examples:
	//   - "/" matches any request
	//   - "/{$}" matches only "/"
	//   - "/static/" matches request whose path begins with "/static/"
	//   - "/index.html" matches the path "/index.html"
	//
	// For details see "Patterns" in https://pkg.go.dev/net/http#ServeMux
	RulePathPrefix = RuleType("pathPrefix")
)

// RuleType specifies URLs matching mechanism for Rule.Value.
type RuleType string

// Rule specifies which authentication Providers should be used for matched data app URLs.
type Rule struct {
	Type         RuleType      `json:"type"`
	Value        string        `json:"value"`
	Auth         []provider.ID `json:"auth"`
	AuthRequired *bool         `json:"authRequired"`
}

func (r *Rule) Match(req *http.Request) (bool, error) {
	switch r.Type {
	case RulePathPrefix:
		if !strings.HasPrefix(r.Value, "/") {
			return false, errors.Errorf(`rule "%s": value "%v" must start with "/"`, r.Type, r.Value)
		}
		if strings.HasSuffix(r.Value, "/") {
			// Rule ends with "/", do prefix match
			return strings.HasPrefix(req.URL.Path, r.Value), nil
		} else {
			// Rule doesn't end with "/", do exact match
			return req.URL.Path == r.Value, nil
		}
	default:
		return false, errors.Errorf(`unexpected data app auth rule "%s"`, r.Type)
	}
}
