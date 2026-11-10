package apptoken_test

import (
	"encoding/json"
	"strings"
	"testing"

	oauthsessions "github.com/oauth2-proxy/oauth2-proxy/v7/pkg/apis/sessions"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/apptoken"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

const droppedMetric = "keboola.go.appsproxy.apptoken.attrs.dropped"

func attrsSession(claims map[string]any) *oauthsessions.SessionState {
	return &oauthsessions.SessionState{AdditionalClaims: claims}
}

// attrsOf returns the `attrs` claim of the token the app gets, nil when the token has none.
func (e *testEnv) attrsOf(got upstreamRequest) map[string]any {
	e.t.Helper()
	tok := e.parse(got.header.Get(apptoken.HeaderName))
	attrs, found := tok.payload["attrs"]
	if !found {
		return nil
	}
	out, ok := attrs.(map[string]any)
	require.True(e.t, ok)
	return out
}

// dropped returns what the counter of dropped claims has counted, by reason.
func (e *testEnv) dropped() map[string]int64 {
	e.t.Helper()
	out := map[string]int64{}
	metrics := e.mocked.TestTelemetry().Metrics(e.t, telemetry.WithMetricFilter(func(m metricdata.Metrics) bool { return m.Name == droppedMetric }))
	for _, m := range metrics {
		sum, ok := m.Data.(metricdata.Sum[int64])
		require.True(e.t, ok)
		for _, point := range sum.DataPoints {
			require.Equal(e.t, 1, point.Attributes.Len(), "the reason is the only label")
			reason, found := point.Attributes.Value("reason")
			require.True(e.t, found)
			out[reason.AsString()] += point.Value
		}
	}
	return out
}

func TestAttrs_MintedFromTheSession(t *testing.T) {
	t.Parallel()
	e := newEnv(t, envOptions{})
	h := e.newHandlerWith(testIDP, []string{"department", "country", "missing"})
	h.session = attrsSession(map[string]any{
		"department": "finance",
		"country":    []any{"CZ", "SK", "CZ"},
		"unlisted":   "secret",
		"sub":        "forged-sub",
		"email":      "forged@example.com",
	})

	got := h.serve(identityHeaders())

	tok := e.parse(got.header.Get(apptoken.HeaderName))
	assert.Equal(t, map[string]any{"department": []any{"finance"}, "country": []any{"CZ", "SK"}}, tok.payload["attrs"])
	assert.Equal(t, map[string][]string{"department": {"finance"}, "country": {"CZ", "SK"}}, tok.claims.Attrs)
	// What is not allowlisted does not reach the token, and a claim of the session never overrides the identity.
	assert.Equal(t, "sub-1", tok.payload["sub"])
	assert.Equal(t, "jane@example.com", tok.payload["email"])
	assert.NotContains(t, string(payloadJSON(t, tok.payload)), "secret")
	assert.Empty(t, e.dropped())
}

func TestAttrs_NoAllowlistNoAttrs(t *testing.T) {
	t.Parallel()
	e := newEnv(t, envOptions{})

	withSession := e.newHandlerWith(testIDP, nil)
	withSession.session = attrsSession(map[string]any{"department": "finance"})
	plain := e.newHandler(testIDP)

	tokWithSession := e.parse(withSession.serve(identityHeaders()).header.Get(apptoken.HeaderName))
	tokPlain := e.parse(plain.serve(identityHeaders()).header.Get(apptoken.HeaderName))

	// The token of an app without an allowlist is the token of today, whatever the session holds.
	assert.NotContains(t, tokWithSession.payload, "attrs")
	assert.NotContains(t, tokPlain.payload, "attrs")
	assert.ElementsMatch(t, keysOf(tokPlain.payload), keysOf(tokWithSession.payload))
	assert.Equal(t, tokPlain.header, tokWithSession.header)
}

func TestAttrs_NothingToMint(t *testing.T) {
	t.Parallel()
	e := newEnv(t, envOptions{})

	t.Run("no session in the request", func(t *testing.T) {
		t.Parallel()
		h := e.newHandlerWith(testIDP, []string{"department"})
		got := h.serve(identityHeaders())
		assert.NotEmpty(t, got.header.Get(apptoken.HeaderName), "the app still gets a token")
		assert.Nil(t, e.attrsOf(got))
	})

	t.Run("a session without the claims", func(t *testing.T) {
		t.Parallel()
		h := e.newHandlerWith(testIDP, []string{"department"})
		h.session = attrsSession(nil)
		got := h.serve(identityHeaders())
		assert.Nil(t, e.attrsOf(got))
	})
}

func TestAttrs_UnusableValuesAreCountedByReason(t *testing.T) {
	t.Parallel()
	e := newEnv(t, envOptions{})
	h := e.newHandlerWith(testIDP, []string{"number", "empty", "long", "good", "many"})
	many := make([]any, 40)
	for i := range many {
		many[i] = "v" + strings.Repeat("x", i)
	}
	h.session = attrsSession(map[string]any{
		"number": float64(7),
		"empty":  "",
		"long":   strings.Repeat("a", 300),
		"good":   "ok",
		"many":   many,
	})

	got := h.serve(identityHeaders())

	assert.Equal(t, map[string]any{"good": []any{"ok"}}, e.attrsOf(got))
	assert.Equal(t, map[string]int64{"type": 2, "size": 2}, e.dropped())
}

func TestAttrs_BudgetOfTheToken(t *testing.T) {
	t.Parallel()

	// The most groups apps-proxy passes on: 4096 B joined with commas.
	groups := strings.TrimSuffix(strings.Repeat("0123456789abcdef,", 4096/17), ",")
	require.LessOrEqual(t, len(groups), 4096)

	attrs := func(n int) map[string]any {
		out := map[string]any{}
		for i := range n {
			// About 1 KB of JSON: it fits the cap of attrs, but not next to the largest groups.
			out["claim"+strings.Repeat("c", i)] = []any{strings.Repeat("w", 248), strings.Repeat("x", 248), strings.Repeat("y", 248), strings.Repeat("z", 248)}
		}
		return out
	}
	names := func(n int) []string {
		out := make([]string, n)
		for i := range out {
			out[i] = "claim" + strings.Repeat("c", i)
		}
		return out
	}

	t.Run("the largest groups: attrs give way, the token stays within the budget", func(t *testing.T) {
		t.Parallel()
		e := newEnv(t, envOptions{})
		h := e.newHandlerWith(testIDP, names(3))
		h.session = attrsSession(attrs(3))
		headers := identityHeaders()
		headers["X-Kbc-User-Roles"] = groups
		// The longest email the verifier accepts, and with it a token like the real ones of an Entra ID tenant.
		headers["X-Kbc-User-Email"] = strings.Repeat("e", 240) + "@example.com"

		got := h.serve(headers)

		token := got.header.Get(apptoken.HeaderName)
		assert.LessOrEqual(t, len(token), 8000)
		tok := e.parse(token)
		assert.Len(t, tok.payload["groups"], 240, "the groups are not cut")
		assert.NotContains(t, tok.payload, "attrs")
		assert.Equal(t, int64(3), e.dropped()["budget"], "every claim was dropped for the budget")
	})

	t.Run("no groups: the whole attrs cap of 1 KB is used, the token is far below the budget", func(t *testing.T) {
		t.Parallel()
		e := newEnv(t, envOptions{})
		h := e.newHandlerWith(testIDP, names(3))
		h.session = attrsSession(attrs(3))

		got := h.serve(without(identityHeaders(), "X-Kbc-User-Roles"))

		token := got.header.Get(apptoken.HeaderName)
		assert.Less(t, len(token), 4000)
		raw, err := json.Marshal(e.attrsOf(got))
		require.NoError(t, err)
		assert.LessOrEqual(t, len(raw), 1024)
		assert.Len(t, e.attrsOf(got), 1, "one claim of about 1 KB fills the cap")
		assert.Equal(t, int64(2), e.dropped()["budget"])
	})

	t.Run("deterministic: the same session gives the same attrs every time", func(t *testing.T) {
		t.Parallel()
		e := newEnv(t, envOptions{})
		h := e.newHandlerWith(testIDP, names(3))
		h.session = attrsSession(attrs(3))
		first, err := json.Marshal(e.attrsOf(h.serve(identityHeaders())))
		require.NoError(t, err)
		for range 50 {
			next, err := json.Marshal(e.attrsOf(h.serve(identityHeaders())))
			require.NoError(t, err)
			require.JSONEq(t, string(first), string(next))
		}
	})
}

// TestAttrs_TokenNeverOverTheBudget sweeps the size of the attributes across the 8000 byte budget next to the largest groups: the size of
// the token is worked out before it is signed, so no token may be over the budget, and the attributes that fit are kept.
func TestAttrs_TokenNeverOverTheBudget(t *testing.T) {
	t.Parallel()
	e := newEnv(t, envOptions{})
	groups := strings.TrimSuffix(strings.Repeat("0123456789abcdef,", 4096/17), ",")

	kept, dropped := 0, 0
	for length := 1; length <= 256; length += 5 {
		h := e.newHandlerWith(testIDP, []string{"a"})
		value := strings.Repeat("v", length)
		h.session = attrsSession(map[string]any{"a": []any{value + "1", value + "2", value + "3", value + "4"}})
		headers := identityHeaders()
		headers["X-Kbc-User-Roles"] = groups
		headers["X-Kbc-User-Email"] = strings.Repeat("e", 240) + "@example.com" // the longest email the verifier accepts

		token := h.serve(headers).header.Get(apptoken.HeaderName)

		require.LessOrEqual(t, len(token), 8000, "values of %d characters", length)
		if _, found := e.parse(token).payload["attrs"]; found {
			kept++
		} else {
			dropped++
		}
	}
	assert.Positive(t, kept, "small attributes fit next to the groups")
	assert.Positive(t, dropped, "large attributes do not")
}

func TestAttrs_ForgedTokenIsReplaced(t *testing.T) {
	t.Parallel()
	e := newEnv(t, envOptions{})
	h := e.newHandlerWith(testIDP, []string{"department"})
	h.session = attrsSession(map[string]any{"department": "finance"})

	headers := identityHeaders()
	headers[apptoken.HeaderName] = "forged"
	got := h.serve(headers)

	require.Len(t, got.header.Values(apptoken.HeaderName), 1)
	assert.Equal(t, map[string]any{"department": []any{"finance"}}, e.attrsOf(got))
}

func TestAttrs_NoValueInTheLogs(t *testing.T) {
	t.Parallel()
	e := newEnv(t, envOptions{})
	h := e.newHandlerWith(testIDP, []string{"department", "number", "long"})
	h.session = attrsSession(map[string]any{"department": "very-secret-value", "number": float64(7), "long": strings.Repeat("s", 300)})

	h.serve(identityHeaders())

	logs := e.mocked.DebugLogger().AllMessages()
	assert.NotContains(t, logs, "very-secret-value")
	assert.NotContains(t, logs, strings.Repeat("s", 20))
}

func payloadJSON(t *testing.T, v any) []byte {
	t.Helper()
	raw, err := json.Marshal(v)
	require.NoError(t, err)
	return raw
}

func keysOf(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
