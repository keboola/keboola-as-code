package proxy_test

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8stypes "k8s.io/apimachinery/pkg/types"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/k8sapp"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/testutil"
)

func routedClaim(name, domain, appID string, routed bool) *unstructured.Unstructured {
	status := "False"
	if routed {
		status = "True"
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": k8sapp.Group + "/" + k8sapp.Version,
		"kind":       "CustomAppDomain",
		"metadata":   map[string]any{"name": name, "namespace": "keboola"},
		"spec":       map[string]any{"domain": domain, "appId": appID, "appRef": "app-" + appID},
		"status": map[string]any{
			"routedAt":   "2026-11-27T10:00:00Z",
			"conditions": []any{map[string]any{"type": "Routed", "status": status, "reason": "EdgeAcquired"}},
		},
	}}
}

// customHostClient sends every request to the proxy, whatever the host, and follows no redirect.
func customHostClient(t *testing.T, proxyURL *url.URL) *http.Client {
	t.Helper()
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{Timeout: 30 * time.Second}).DialContext(ctx, network, proxyURL.Host)
	}
	transport.Proxy = nil
	transport.TLSClientConfig.InsecureSkipVerify = true
	return &http.Client{
		Transport:     transport,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}

func TestCustomHostsThroughTheProxy(t *testing.T) {
	t.Parallel()

	pm := osAssignedPorts{}
	ctx := t.Context()
	appsAPI := testutil.StartDataAppsAPI(t, pm)
	t.Cleanup(appsAPI.Close)
	appServer := testutil.StartAppServer(t, pm)
	t.Cleanup(appServer.Close)

	apps := testDataApps(testutil.AppServerURL(t, appServer), testAuthProviders(t, pm))
	appsAPI.Register(apps)

	objects := makeDefaultK8sObjects(apps, testutil.AppServerURL(t, appServer).String())
	objects = append(objects,
		routedClaim("123-aaaa", "Data.Customer.Org", "123", true),
		// A claim under the proxy's own host must never shadow a .hub URL.
		routedClaim("auth-bbbb", "auth.hub.keboola.local", "123", true),
	)
	d, mocked := createDependencies(t, ctx, appsAPI.URL, objects...)
	handler := createProxyHandler(ctx, d)

	srv := httptest.NewUnstartedServer(handler)
	srv.Config.ErrorLog = log.NewStdErrorLogger(d.Logger())
	srv.StartTLS()
	t.Cleanup(srv.Close)
	proxyURL, err := url.Parse(srv.URL)
	require.NoError(t, err)
	client := customHostClient(t, proxyURL)

	require.True(t, d.AppStateWatcher().WaitForCacheSync(ctx))
	require.Eventually(t, d.CustomHosts().HasSynced, 5*time.Second, 20*time.Millisecond)
	require.Eventually(t, func() bool { _, ok := d.CustomHosts().Resolve("data.customer.org"); return ok }, 5*time.Second, 20*time.Millisecond)

	get := func(target, xForwardedHost string) (*http.Response, string) {
		t.Helper()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
		require.NoError(t, err)
		if xForwardedHost != "" {
			req.Header.Set("X-Forwarded-Host", xForwardedHost)
		}
		resp, err := client.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		return resp, string(body)
	}

	// The custom host routes to its app, with no canonical redirect.
	resp, body := get("https://data.customer.org/", "")
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "Hello, client", body)
	require.Len(t, *appServer.Requests, 1)
	assert.Equal(t, 1, d.AppHandlers().HandlerCount())

	// An unknown host is rejected without reaching an app, and a spoofed X-Forwarded-Host does not make it a known one.
	resp, body = get("https://unknown.customer.org/", "data.customer.org")
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Contains(t, body, "Unexpected domain, missing application ID.")
	assert.Len(t, *appServer.Requests, 1)

	// A claim under the proxy's own host is not indexed.
	_, ok := d.CustomHosts().Resolve("auth.hub.keboola.local")
	assert.False(t, ok)

	// An App that publishes a custom URL itself is not routed by it: only a Routed claim routes a host.
	_, err = mocked.TestFakeK8sClient().Resource(k8sapp.AppGVR()).Namespace("keboola").Patch(ctx, "app-123", k8stypes.MergePatchType,
		[]byte(`{"status":{"appsProxy":{"publicUrl":"https://other.customer.org"}}}`), metav1.PatchOptions{})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		info, _ := d.AppStateWatcher().GetState(k8sapp.WorkloadRef{AppID: "123"})
		return info.PublicHost == "other.customer.org"
	}, 5*time.Second, 20*time.Millisecond)
	resp, body = get("https://other.customer.org/", "")
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Contains(t, body, "Unexpected domain, missing application ID.")
	assert.Len(t, *appServer.Requests, 1)

	// A stopped app is woken by a request on its custom host.
	patchStoppedApp("123", `{"spec": {"state": "Stopped", "autoRestartEnabled": true}, "status": {"currentState": "Stopped"}}`)(t, mocked.TestFakeK8sClient(), d.AppStateWatcher())
	patchesBefore := appPatchCount(mocked.TestFakeK8sClient(), "123")
	resp, body = get("https://data.customer.org/", "")
	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	assert.Contains(t, body, "<title>Starting</title>")
	require.Eventually(t, func() bool {
		return appPatchCount(mocked.TestFakeK8sClient(), "123") > patchesBefore
	}, 5*time.Second, 50*time.Millisecond, "expected a wake patch of the App CR")

	// A claim that stops being Routed stops routing.
	_, err = mocked.TestFakeK8sClient().Resource(k8sapp.CustomAppDomainGVR()).Namespace("keboola").Update(ctx, routedClaim("123-aaaa", "data.customer.org", "123", false), metav1.UpdateOptions{})
	require.NoError(t, err)
	require.Eventually(t, func() bool { _, ok := d.CustomHosts().Resolve("data.customer.org"); return !ok }, 5*time.Second, 20*time.Millisecond)
	resp, _ = get("https://data.customer.org/", "")
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	// The handler cached for the host is evicted too.
	assert.Eventually(t, func() bool { return d.AppHandlers().HandlerCount() == 0 }, 5*time.Second, 20*time.Millisecond)
}
