package service

import (
	"testing"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/gen/buffer"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testproject"
	"github.com/stretchr/testify/assert"
)

func TestTokenRefresh(t *testing.T) {
	t.Parallel()

	// Create service connected to a testing project
	prj := testproject.GetTestProjectForTest(t)
	deps := dependencies.NewMockedDeps(t, dependencies.WithTestProject(prj))
	svc := New(deps)
	str := deps.Store()

	// Create receiver
	receiver, err := svc.CreateReceiver(deps, &buffer.CreateReceiverPayload{
		Name: "Refresh Tokens Test",
		Exports: []*buffer.CreateExportData{
			{
				Name: "Export 1",
				Mapping: &buffer.Mapping{
					TableID: "in.c-refresh-tokens.table",
					Columns: []*buffer.Column{
						{
							Type: "id",
							Name: "id",
						},
					},
				},
				Conditions: &buffer.Conditions{
					Count: 1000,
					Size:  "100KB",
					Time:  "5m0s",
				},
			},
		},
	})
	assert.NoError(t, err)

	oldTokens, err := str.ListTokens(deps.RequestCtx(), key.ReceiverKey{ProjectID: deps.ProjectID(), ReceiverID: string(receiver.ID)})

	time.Sleep(time.Second)

	// Refresh tokens
	_, err = svc.RefreshReceiverTokens(deps, &buffer.RefreshReceiverTokensPayload{ReceiverID: receiver.ID})
	assert.NoError(t, err)

	newTokens, err := str.ListTokens(deps.RequestCtx(), key.ReceiverKey{ProjectID: deps.ProjectID(), ReceiverID: string(receiver.ID)})

	// Assert that tokens were refreshed
	for i, oldToken := range oldTokens {
		newToken := newTokens[i]
		assert.Equal(t, oldToken.ID, newToken.ID)
		assert.NotEqual(t, oldToken.Refreshed, newToken.Refreshed)
	}
}
