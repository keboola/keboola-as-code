package recordctx

import (
	jsoniter "github.com/json-iterator/go"
	"github.com/keboola/go-utils/pkg/orderedmap"

	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/httputils"
	utilsUrl "github.com/keboola/keboola-as-code/internal/pkg/utils/url"
)

// json - replacement of the standard encoding/json library, it is faster for larger responses.
var json = jsoniter.ConfigCompatibleWithStandardLibrary //nolint:gochecknoglobals

func parseBody(contentType string, body []byte) (data *orderedmap.OrderedMap, err error) {
	// Decode
	switch {
	case httputils.IsContentTypeForm(contentType):
		data, err = utilsUrl.ParseQuery(string(body))
		if err != nil {
			return nil, serviceError.NewBadRequestError(errors.Errorf("invalid form data: %w", err))
		}
	case httputils.IsContentTypeJSON(contentType):
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
