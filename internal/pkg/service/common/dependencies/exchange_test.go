package dependencies

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	svcerrors "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
)

func TestIsProgrammaticToken(t *testing.T) {
	t.Parallel()
	assert.True(t, IsProgrammaticToken("kbc_at_secret"))
	assert.True(t, IsProgrammaticToken("kbc_pat_secret"))
	assert.True(t, IsProgrammaticToken("Bearer kbc_at_secret"))
	assert.False(t, IsProgrammaticToken("regular-storage-token"))
	assert.False(t, IsProgrammaticToken(""))
}

func TestConnectionURL(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "https://connection.keboola.com", connectionURL("connection.keboola.com"))
	assert.Equal(t, "https://connection.keboola.com", connectionURL("https://connection.keboola.com"))
	assert.Equal(t, "http://127.0.0.1:8080", connectionURL("http://127.0.0.1:8080"))
}

func TestExchangeProgrammaticToken_NoConfig(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	d := NewMocked(t, ctx)

	// Empty service account token path disables the feature.
	_, err := ExchangeProgrammaticToken(ctx, d, "", "kbc_at_secret", 12345)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not configured")

	// The failure must surface as a client error (400), not the default 500 — otherwise
	// an unsupported-auth request gets logged at Error level and alerted on.
	assert.Equal(t, http.StatusBadRequest, svcerrors.HTTPCodeFrom(err))
}
