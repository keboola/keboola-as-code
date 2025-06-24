package test

import (
	"io"
	"net/http"
	"strconv"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/sasha-s/go-deadlock"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func MockTokenStorageAPICalls(tb testing.TB, transport *httpmock.MockTransport) {
	tb.Helper()
	lock := &deadlock.Mutex{}

	tokenCounter := 1000
	transport.RegisterResponder(
		http.MethodPost,
		`=~/v2/storage/tokens$`,
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

	transport.RegisterResponder(
		http.MethodDelete,
		`=~/v2/storage/tokens/[0-9]+$`,
		func(request *http.Request) (*http.Response, error) {
			lock.Lock()
			defer lock.Unlock()

			tokenID, err := extractTokenIDFromURL(request.URL.String())
			if err != nil {
				return nil, err
			}

			tokenIDInt, err := strconv.Atoi(tokenID)
			if err != nil {
				return nil, err
			}

			if tokenIDInt > tokenCounter {
				return nil, errors.Errorf(`unexpected token ID "%d"`, tokenIDInt)
			}

			return httpmock.NewStringResponse(http.StatusNoContent, ""), nil
		},
	)
}
