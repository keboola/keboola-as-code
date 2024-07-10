package recordctx

import (
	"encoding/json"
	"net/http"
	"sort"
	"strings"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/umisama/go-regexpcache"

	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	utilsUrl "github.com/keboola/keboola-as-code/internal/pkg/utils/url"
)

func isContentTypeJSON(t string) bool {
	return regexpcache.MustCompile(`^application/([a-zA-Z0-9\.\-]+\+)?json$`).MatchString(t)
}

func isContentTypeForm(t string) bool {
	return strings.HasPrefix(t, "application/x-www-form-urlencoded")
}

func headersToMap(in http.Header) *orderedmap.OrderedMap {
	out := orderedmap.New()
	for k, v := range in {
		out.Set(http.CanonicalHeaderKey(k), v[0])
	}
	out.SortKeys(func(keys []string) {
		sort.Strings(keys)
	})
	return out
}

func parseBody(header http.Header, body string) (data *orderedmap.OrderedMap, err error) {
	contentType := header.Get("Content-Type")
	// Decode
	switch {
	case isContentTypeForm(contentType):
		data, err = utilsUrl.ParseQuery(body)
		if err != nil {
			return nil, serviceError.NewBadRequestError(errors.Errorf("invalid form data: %w", err))
		}
	case isContentTypeJSON(contentType):
		err = json.Unmarshal([]byte(body), &data)
		if err != nil {
			return nil, serviceError.NewBadRequestError(errors.Errorf("invalid JSON: %w", err))
		}
	default:
		return nil, serviceError.NewUnsupportedMediaTypeError(errors.Errorf(
			`unsupported content type "%s", supported types: application/json and application/x-www-form-urlencoded`,
			contentType,
		))
	}

	return data, nil
}
