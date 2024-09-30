package transport

import (
	"fmt"
	"io"

	"github.com/ccoveille/go-safecast"
	"github.com/hashicorp/yamux"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network"
)

func sessionKey(session *yamux.Session) string {
	return session.RemoteAddr().String()
}

func streamKey(stream *ServerStream) string {
	return fmt.Sprintf(`%s-%d`, stream.RemoteAddr(), stream.StreamID())
}

func multiplexerConfig(_ log.Logger, config network.Config) *yamux.Config {
	maxWindowSize, err := safecast.ToUint32(config.StreamMaxWindow.Bytes())
	if err != nil {
		panic(err)
	}

	return &yamux.Config{
		AcceptBacklog:          config.MaxWaitingStreams,
		EnableKeepAlive:        true,
		KeepAliveInterval:      config.KeepAliveInterval,
		ConnectionWriteTimeout: config.StreamWriteTimeout,
		MaxStreamWindowSize:    maxWindowSize,
		StreamOpenTimeout:      config.StreamOpenTimeout,
		StreamCloseTimeout:     config.StreamCloseTimeout,
		// Disable logs, prevent duplicate error logs.
		// Logger: log.NewStdErrorLogger(logger.WithComponent("mux")),
		LogOutput: io.Discard,
	}
}
