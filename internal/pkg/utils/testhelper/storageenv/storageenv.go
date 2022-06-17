package storageenv

import (
	"strings"

	"github.com/umisama/go-regexpcache"

	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
)

type storageEnvTicketProvider struct {
	api  *storageapi.Api
	envs *env.Map
}

// StorageEnvTicketProvider allows you to generate new unique IDs via an ENV variable in the test.
func CreateStorageEnvTicketProvider(api *storageapi.Api, envs *env.Map) testhelper.EnvProvider {
	return &storageEnvTicketProvider{api, envs}
}

func (p *storageEnvTicketProvider) MustGet(key string) string {
	key = strings.Trim(key, "%")
	nameRegexp := regexpcache.MustCompile(`^TEST_NEW_TICKET_\d+$`)
	if _, found := p.envs.Lookup(key); !found && nameRegexp.MatchString(key) {
		ticket, err := p.api.GenerateNewId()
		if err != nil {
			panic(err)
		}

		p.envs.Set(key, ticket.Id)
		return ticket.Id
	}

	return p.envs.MustGet(key)
}
