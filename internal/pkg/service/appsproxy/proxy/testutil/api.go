package testutil

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/mitchellh/hashstructure/v2"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
)

type DataAppsAPI struct {
	*httptest.Server
	Apps          map[api.AppID]api.AppConfig
	Notifications map[string]int
	WakeUps       map[string]int
}

func StartDataAppsAPI(t *testing.T) *DataAppsAPI {
	t.Helper()

	service := &DataAppsAPI{
		Apps:          make(map[api.AppID]api.AppConfig),
		Notifications: make(map[string]int),
		WakeUps:       make(map[string]int),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /apps/{app}/proxy-config", func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		appID := req.PathValue("app")
		app, ok := service.Apps[api.AppID(appID)]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			_, _ = fmt.Fprintln(w, "{}")
			return
		}

		expectedETag := strings.Trim(req.Header.Get("If-None-Match"), `"`)
		actualETagInt, err := hashstructure.Hash(app, hashstructure.FormatV2, &hashstructure.HashOptions{})
		require.NoError(t, err)
		actualETag := strconv.FormatUint(actualETagInt, 10)

		w.Header().Set("ETag", fmt.Sprintf(`"%s"`, actualETag))
		if expectedETag == actualETag {
			w.WriteHeader(http.StatusNotModified)
			_, _ = fmt.Fprintln(w, "{}")
			return
		}

		w.WriteHeader(http.StatusOK)
		jsonData, err := json.Encode(app, true)
		require.NoError(t, err)
		_, _ = w.Write(jsonData)
	})
	mux.HandleFunc("PATCH /apps/{app}", func(w http.ResponseWriter, req *http.Request) {
		appID := req.PathValue("app")

		body, err := io.ReadAll(req.Body)
		require.NoError(t, err)

		data := make(map[string]string)
		err = json.DecodeString(string(body), &data)
		require.NoError(t, err)

		if _, ok := data["lastRequestTimestamp"]; ok {
			service.Notifications[appID] += 1
		}
		if _, ok := data["desiredState"]; ok {
			service.WakeUps[appID] += 1
		}
	})

	ts := httptest.NewUnstartedServer(mux)
	ts.EnableHTTP2 = true
	ts.Start()

	service.Server = ts

	return service
}

func (v *DataAppsAPI) Register(apps []api.AppConfig) {
	for _, app := range apps {
		v.Apps[app.ID] = app
	}
}
