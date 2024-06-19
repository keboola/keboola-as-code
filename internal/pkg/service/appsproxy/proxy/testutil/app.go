package testutil

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"nhooyr.io/websocket"
	"nhooyr.io/websocket/wsjson"
)

type AppServer struct {
	*httptest.Server
	Requests *[]*http.Request
}

func StartAppServer(t *testing.T) *AppServer {
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

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		lock.Lock()
		defer lock.Unlock()
		requests = append(requests, r)
		_, _ = fmt.Fprint(w, "Hello, client")
	})

	ts := httptest.NewUnstartedServer(mux)
	ts.EnableHTTP2 = true
	ts.Start()

	return &AppServer{ts, &requests}
}
