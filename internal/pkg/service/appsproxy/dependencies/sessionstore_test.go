package dependencies

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func TestMockedServiceScope_SessionStore_Default(t *testing.T) {
	t.Parallel()

	d, _ := NewMockedServiceScope(t, t.Context(), config.New())
	require.NotNil(t, d.SessionStoreClient())
	require.NoError(t, d.SessionStoreClient().Ping(t.Context()).Err())
	assert.NotNil(t, d.SessionStoreChecker())
	assert.NotNil(t, d.SessionStoreMetrics())
}

func TestServiceScope_SessionStoreClient_ClosedOnShutdown(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	d, _ := NewMockedServiceScopeWithSessionStore(t, t.Context(), config.New(), client, nil)
	require.Same(t, client, d.SessionStoreClient())
	require.NoError(t, client.Ping(t.Context()).Err())

	d.Process().Shutdown(context.WithoutCancel(t.Context()), errors.New("bye bye"))
	d.Process().WaitForShutdown()
	require.ErrorIs(t, client.Ping(t.Context()).Err(), redis.ErrClosed)
}
