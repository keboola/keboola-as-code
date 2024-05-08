package test

import (
	"io"
	"net/http"
	"strconv"
	"sync"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
)

func MockTokenStorageAPICalls(t *testing.T, transport *httpmock.MockTransport) {
	t.Helper()
	lock := &sync.Mutex{}

	tokenCounter := 1000
	transport.RegisterResponder(
		http.MethodPost,
		`=~/v2/storage/tokens`,
		func(request *http.Request) (*http.Response, error) {
			lock.Lock()
			defer lock.Unlock()

			dataBytes, err := io.ReadAll(request.Body)
			if err != nil {
				return nil, err
			}

			token := keboola.Token{}
			if err := json.Decode(dataBytes, &token); err != nil {
				return nil, err
			}

			tokenCounter++
			token.ID = strconv.Itoa(tokenCounter)
			token.Token = "my-token"
			return httpmock.NewJsonResponse(http.StatusCreated, token)
		},
	)
}
