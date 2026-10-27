package upstream

import (
	"net/http"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/k8sapp"
)

// serveStatic never wakes the App: a static App has no workload, and one that is not
// Running has no upstream the static server would accept.
func (u *AppUpstream) serveStatic(rw http.ResponseWriter, req *http.Request) error {
	info, ok := u.manager.stateWatcher.GetState(req.Context(), u.workload)
	if !ok {
		// The state cache is still filling; the spinner page reloads itself.
		u.manager.pageWriter.WriteSpinnerPage(rw, req, u.app)
		return nil
	}
	if info.ActualState != k8sapp.AppActualStateRunning || u.target == nil {
		u.manager.pageWriter.WriteErrorPage(rw, req, &u.app, http.StatusServiceUnavailable, "The app is not available.", "")
		return nil
	}

	limit := u.manager.staticRequests.GetOrInit(u.app.ID)
	if !limit.tryAcquire() {
		rw.Header().Set("Retry-After", "1")
		u.manager.pageWriter.WriteErrorPage(rw, req, &u.app, http.StatusServiceUnavailable, "Too many concurrent requests to this app.", "")
		return nil
	}
	defer limit.release()

	return u.handler.ServeHTTPOrError(rw, req)
}

// requestLimit caps the concurrent requests to one static app.
type requestLimit struct {
	slots chan struct{}
}

func (l *requestLimit) tryAcquire() bool {
	select {
	case l.slots <- struct{}{}:
		return true
	default:
		return false
	}
}

func (l *requestLimit) release() {
	<-l.slots
}

func (u *AppUpstream) transport() http.RoundTripper {
	if u.static {
		return u.manager.staticTransport
	}
	return u.manager.transport
}

// stripCORSHeaders keeps a static app's response unreadable to other apps' pages: all
// app hostnames are same-site, so the viewer's session cookie is sent to every app.
func stripCORSHeaders(res *http.Response) {
	for name := range res.Header {
		if strings.HasPrefix(strings.ToLower(name), "access-control-") {
			res.Header.Del(name)
		}
	}
}
