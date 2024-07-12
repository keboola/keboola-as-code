package recordctx

import (
	"strings"

	jsoniter "github.com/json-iterator/go"
	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/umisama/go-regexpcache"

	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	utilsUrl "github.com/keboola/keboola-as-code/internal/pkg/utils/url"
)

// json - replacement of the standard encoding/json library, it is faster for larger responses.
var json = jsoniter.ConfigCompatibleWithStandardLibrary //nolint:gochecknoglobals

func isContentTypeJSON(t string) bool {
	return regexpcache.MustCompile(`^application/([a-zA-Z0-9\.\-]+\+)?json$`).MatchString(t)
}

func isContentTypeForm(t string) bool {
	return strings.HasPrefix(t, "application/x-www-form-urlencoded")
}

func parseBody(contentType string, body []byte) (data *orderedmap.OrderedMap, err error) {
	// Decode
	switch {
	case isContentTypeForm(contentType):
		data, err = utilsUrl.ParseQuery(string(body))
		if err != nil {
			return nil, serviceError.NewBadRequestError(errors.Errorf("invalid form data: %w", err))
		}
	case isContentTypeJSON(contentType):
		err = json.Unmarshal(body, &data)
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
