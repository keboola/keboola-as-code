package transport_test

import (
	"testing"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network/transport"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network/transport/kcp"
)

func TestTransport_SmallData_KCP(t *testing.T) {
	t.Parallel()
	testTransportSmallData(t, func(cfg network.Config) transport.Protocol { return kcp.New(cfg) })
}

func TestTransportBiggerData_KCP(t *testing.T) {
	t.Parallel()
	testTransportBiggerData(t, func(cfg network.Config) transport.Protocol { return kcp.New(cfg) })
}
