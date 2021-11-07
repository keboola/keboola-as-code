package remote_test

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/testproject"
)

func TestGenerateNewId(t *testing.T) {
	t.Parallel()
	project := testproject.GetTestProject(t, env.Empty())
	api := project.StorageApi()

	ticket, err := api.GenerateNewId()
	assert.NoError(t, err)
	assert.NotNil(t, ticket)
	assert.NotEmpty(t, ticket.Id)
}

func TestTicketProvider(t *testing.T) {
	t.Parallel()
	project := testproject.GetTestProject(t, env.Empty())
	api := project.StorageApi()

	tickets := api.NewTicketProvider()
	values := make([]string, 0)

	// Request 3 tickets
	for i := 0; i < 3; i++ {
		tickets.Request(func(ticket *model.Ticket) {
			values = append(values, ticket.Id)
		})
	}

	// Start HTTP pool, wait for all responses
	assert.NoError(t, tickets.Resolve())

	// Assert order
	expected := make([]string, len(values))
	copy(expected, values)
	sort.Strings(expected)
	assert.Equal(t, expected, values)
}
