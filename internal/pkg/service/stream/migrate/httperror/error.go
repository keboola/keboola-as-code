package httperror

import (
	"encoding/json"
	"io"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type HTTPError struct {
	StatusCode int    `json:"statusCode"`
	HTTPError  string `json:"error"`
	Message    string `json:"message"`
}

func (e *HTTPError) Error() string {
	return ""
}

func Parser(body io.Reader) error {
	// Read the response body into bytes
	bodyBytes, err := io.ReadAll(body)
	if err != nil {
		return err
	}

	// Parse the JSON response into HTTPError struct
	var errStruct map[string]interface{}
	if err := json.Unmarshal(bodyBytes, &errStruct); err != nil {
		return errors.New(string(bodyBytes))
	}

	// Marshal the error struct into JSON
	jsonErr, err := json.Marshal(&errStruct)
	if err != nil {
		return err
	}

	return errors.New(string(jsonErr))
}
