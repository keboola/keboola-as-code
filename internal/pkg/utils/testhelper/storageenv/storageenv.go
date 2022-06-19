package storageenv

import (
	"context"
	"strings"

	"github.com/keboola/go-client/pkg/client"
	"github.com/umisama/go-regexpcache"

	"github.com/keboola/go-client/pkg/storageapi"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
)

type storageEnvTicketProvider struct {
	ctx              context.Context
	storageApiClient client.Sender
	envs             *env.Map
}

// CreateStorageEnvTicketProvider allows you to generate new unique IDs via an ENV variable in the test.
func CreateStorageEnvTicketProvider(ctx context.Context, storageApiClient client.Sender, envs *env.Map) testhelper.EnvProvider {
	return &storageEnvTicketProvider{ctx: ctx, storageApiClient: storageApiClient, envs: envs}
}

func (p *storageEnvTicketProvider) MustGet(key string) string {
	key = strings.Trim(key, "%")
	nameRegexp := regexpcache.MustCompile(`^TEST_NEW_TICKET_\d+$`)
	if _, found := p.envs.Lookup(key); !found && nameRegexp.MatchString(key) {
		ticket, err := storageapi.GenerateIDRequest().Send(p.ctx, p.storageApiClient)
		if err != nil {
			panic(err)
		}

		p.envs.Set(key, ticket.ID)
		return ticket.ID
	}

	return p.envs.MustGet(key)
}
