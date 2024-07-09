// Package transport provides transport layer for communication between source and disk writer nodes.
//
// Communication is based on KCP Reliable UDP protocol and yamux streams multiplexer.
// TCP is an old protocol with several issues regarding latency.
// It is not possible to make major changes to TCP, as it is a widely used standard.
// Therefore, R-UDP protocols are used to address issues.
package transport

import (
	"fmt"

	"github.com/hashicorp/yamux"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network"
)

func sessionKey(session *yamux.Session) string {
	return session.RemoteAddr().String()
}

func streamKey(stream *yamux.Stream) string {
	return fmt.Sprintf(`%s-%d`, stream.RemoteAddr(), stream.StreamID())
}

func multiplexerConfig(logger log.Logger, config network.Config) *yamux.Config {
	return &yamux.Config{
		AcceptBacklog:          config.MaxWaitingStreams,
		EnableKeepAlive:        true,
		KeepAliveInterval:      config.KeepAliveInterval,
		ConnectionWriteTimeout: config.StreamWriteTimeout,
		MaxStreamWindowSize:    uint32(config.StreamMaxWindow.Bytes()),
		StreamOpenTimeout:      config.StreamOpenTimeout,
		StreamCloseTimeout:     config.StreamCloseTimeout,
		Logger:                 log.NewStdErrorLogger(logger.WithComponent("mux")),
	}
}
