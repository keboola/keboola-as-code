package storageenvmock

import (
	"context"
	"strconv"
	"strings"

	"github.com/umisama/go-regexpcache"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
)

type storageEnvMockTicketProvider struct {
	ctx      context.Context
	envs     *env.Map
	ticketID int
}

// CreateStorageEnvMockTicketProvider allows you to generate new unique IDs via an ENV variable in the test.
func CreateStorageEnvMockTicketProvider(ctx context.Context, envs *env.Map) testhelper.EnvProvider {
	return &storageEnvMockTicketProvider{ctx: ctx, envs: envs, ticketID: 1}
}

func (p *storageEnvMockTicketProvider) GetOrErr(key string) (string, error) {
	if v := p.getForTicket(key); v != "" {
		return v, nil
	}
	return p.envs.GetOrErr(key)
}

func (p *storageEnvMockTicketProvider) MustGet(key string) string {
	if v := p.getForTicket(key); v != "" {
		return v
	}
	return p.envs.MustGet(key)
}

func (p *storageEnvMockTicketProvider) getForTicket(key string) string {
	key = strings.Trim(key, "%")
	nameRegexp := regexpcache.MustCompile(`^TEST_NEW_TICKET_\d+$`)
	if _, found := p.envs.Lookup(key); !found && nameRegexp.MatchString(key) {
		ticketIDString := strconv.Itoa(p.ticketID)
		p.envs.Set(key, ticketIDString)
		p.ticketID++
		return ticketIDString
	}

	return ""
}
