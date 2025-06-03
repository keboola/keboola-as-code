package storageenv

import (
	"context"
	"strings"

	"github.com/umisama/go-regexpcache"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/ulid"
)

type storageEnvTicketProvider struct {
	ctx  context.Context
	envs *env.Map
}

// CreateStorageEnvTicketProvider allows you to generate new unique IDs via an ENV variable in the test.
func CreateStorageEnvTicketProvider(ctx context.Context, envs *env.Map) testhelper.EnvProvider {
	return &storageEnvTicketProvider{ctx: ctx, envs: envs}
}

func (p *storageEnvTicketProvider) GetOrErr(key string) (string, error) {
	if v := p.getForTicket(key); v != "" {
		return v, nil
	}
	return p.envs.GetOrErr(key)
}

func (p *storageEnvTicketProvider) MustGet(key string) string {
	if v := p.getForTicket(key); v != "" {
		return v
	}
	return p.envs.MustGet(key)
}

func (p *storageEnvTicketProvider) getForTicket(key string) string {
	key = strings.Trim(key, "%")
	nameRegexp := regexpcache.MustCompile(`^TEST_NEW_TICKET_\d+$`)
	if _, found := p.envs.Lookup(key); !found && nameRegexp.MatchString(key) {
		// Generate ULID
		newID := ulid.NewDefaultGenerator().NewULID()

		p.envs.Set(key, newID)
		return newID
	}
	return ""
}
