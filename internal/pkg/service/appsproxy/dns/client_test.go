package dns_test

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dns"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func TestClient_Resolve(t *testing.T) {
	t.Parallel()

	client, err := dns.NewClient()
	require.NoError(t, err)

	// Local DNS server will be unable to resolve keboola.com because of RecursionDesired = false.
	ip, err := client.Resolve(context.Background(), "keboola.com")
	assert.Equal(t, ip, "")

	var dnsErr *net.DNSError
	assert.True(t, errors.As(err, &dnsErr))
	assert.True(t, dnsErr.IsNotFound)
}
