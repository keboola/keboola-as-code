package list

import (
	"net/http"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dependenciesPkg "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
)

func TestRunListsAllPagesSortedByTableID(t *testing.T) {
	t.Parallel()

	d := dependenciesPkg.NewMocked(t, t.Context())
	transport := d.MockedHTTPTransport()
	transport.RegisterResponder(
		http.MethodGet, `=~/storage/dev-branches`,
		httpmock.NewJsonResponderOrPanic(200, []*keboola.Branch{{BranchKey: keboola.BranchKey{ID: 123}, Name: "Main", IsDefault: true}}),
	)
	transport.RegisterResponder(
		http.MethodGet, `=~/v2/storage/branch/123/table-list`,
		func(req *http.Request) (*http.Response, error) {
			body := `{"data":[{"id":"out.c-main.orders","branchId":123,"name":"orders","displayName":"Orders"}],` +
				`"meta":{"total":2,"limit":200},"links":{"self":"","first":"","next":"https://connection.keboola.local/v2/storage/branch/123/table-list?page[after]=c1"}}`
			if req.URL.Query().Get("page[after]") == "c1" {
				body = `{"data":[{"id":"in.c-test.accounts","branchId":123,"name":"accounts","displayName":"accounts"}],` +
					`"meta":{"total":2,"limit":200},"links":{"self":"","first":"","next":""}}`
			}
			resp := httpmock.NewStringResponse(http.StatusOK, body)
			resp.Header.Set("Content-Type", "application/json")
			return resp, nil
		},
	)

	require.NoError(t, Run(t.Context(), d))

	assert.Equal(t, 2, transport.GetCallCountInfo()[`GET =~/v2/storage/branch/123/table-list`])
	d.DebugLogger().AssertJSONMessages(t, `
{"level":"info","message":"Loading tables, please wait."}
{"level":"info","message":"Found tables:"}
{"level":"info","message":"  in.c-test.accounts (Name: accounts, Bucket: in.c-test)"}
{"level":"info","message":"  out.c-main.orders (Name: Orders, Bucket: out.c-main)"}
`)
}
