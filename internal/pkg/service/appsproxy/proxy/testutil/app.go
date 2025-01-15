package testutil

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/server"
)

type AppServer struct {
	*httptest.Server
	Requests *[]*http.Request
}

func StartAppServer(t *testing.T, pm server.PortManager) *AppServer {
	t.Helper()

	lock := &sync.Mutex{}
	var requests []*http.Request

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(r.Context(), time.Second*10)
		defer cancel()

		err = wsjson.Write(ctx, c, "Hello websocket")
		require.NoError(t, err)

		assert.NoError(t, c.Close(websocket.StatusNormalClosure, ""))
	})

	mux.HandleFunc("/ws2", func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(r.Context(), time.Second*15)
		defer cancel()

		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				// Ignore error, the websocket can be closed at this point
				c.Close(websocket.StatusNormalClosure, "Connection closed") //nolint:errcheck
				return
			case <-ticker.C:
				// Ignore error, the websocket can be closed at this point
				wsjson.Write(ctx, c, "Hello from websocket") //nolint:errcheck
			}
		}
	})

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		lock.Lock()
		defer lock.Unlock()
		requests = append(requests, r)
		_, _ = fmt.Fprint(w, "Hello, client")
	})

	port := pm.GetFreePort()
	l, err := net.Listen("tcp", "127.0.0.1:"+strconv.FormatInt(int64(port), 10))
	for err != nil {
		port = pm.GetFreePort()
		l, err = net.Listen("tcp", "127.0.0.1:"+strconv.FormatInt(int64(port), 10))
	}

	ts := &httptest.Server{
		Listener:    l,
		Config:      &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second},
		EnableHTTP2: true,
	}
	ts.Start()

	return &AppServer{ts, &requests}
}
