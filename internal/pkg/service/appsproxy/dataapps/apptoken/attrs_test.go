package apptoken

import (
	"encoding/json"
	"strings"
	"testing"

	sdk "github.com/keboola/keboola-sdk-go/v2/pkg/apptoken"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMapAttrs_Values(t *testing.T) {
	t.Parallel()

	many := make([]any, 1000)
	for i := range many {
		many[i] = "v" + strings.Repeat("x", i%5) + string(rune('a'+i%26)) + strings.Repeat("y", i/26)
	}
	thirtyTwo := make([]any, sdk.MaxAttrValues)
	thirtyThree := make([]any, sdk.MaxAttrValues+1)
	for i := range thirtyThree {
		thirtyThree[i] = "v" + strings.Repeat("a", i)
		if i < len(thirtyTwo) {
			thirtyTwo[i] = thirtyThree[i]
		}
	}

	cases := []struct {
		name   string
		value  any
		want   []string
		reason attrDrop
	}{
		{name: "string", value: "finance", want: []string{"finance"}},
		{name: "list", value: []any{"finance", "hr"}, want: []string{"finance", "hr"}},
		{name: "list of strings", value: []string{"finance", "hr"}, want: []string{"finance", "hr"}},
		{name: "duplicates are removed, the order is canonical", value: []any{"hr", "finance", "hr"}, want: []string{"finance", "hr"}},
		{name: "number", value: float64(42), reason: attrDropType},
		{name: "bool", value: true, reason: attrDropType},
		{name: "null", value: nil, reason: attrDropType},
		{name: "object", value: map[string]any{"a": "b"}, reason: attrDropType},
		{name: "mixed list", value: []any{"finance", float64(1)}, reason: attrDropType},
		{name: "list with an empty string", value: []any{"finance", ""}, reason: attrDropType},
		{name: "empty string", value: "", reason: attrDropType},
		{name: "empty list", value: []any{}, reason: attrDropType},
		{name: "control character", value: "fin\x00ance", reason: attrDropType},
		{name: "invalid UTF-8", value: "fin\xffance", reason: attrDropType},
		{name: "32 values", value: thirtyTwo, want: toStrings(thirtyTwo)},
		{name: "33 values", value: thirtyThree, reason: attrDropSize},
		{name: "a list of 1000", value: many, reason: attrDropSize},
		{name: "1024 characters", value: strings.Repeat("a", sdk.MaxAttrValueChars), want: []string{strings.Repeat("a", sdk.MaxAttrValueChars)}},
		{name: "1025 characters", value: strings.Repeat("a", sdk.MaxAttrValueChars+1), reason: attrDropSize},
		{name: "1024 multibyte characters", value: strings.Repeat("ě", sdk.MaxAttrValueChars), want: []string{strings.Repeat("ě", sdk.MaxAttrValueChars)}},
		{name: "1025 multibyte characters", value: strings.Repeat("ě", sdk.MaxAttrValueChars+1), reason: attrDropSize},
		{name: "4 KB value", value: strings.Repeat("a", 4096), reason: attrDropSize},
		{name: "a claim larger than the whole attrs", value: []any{strings.Repeat("a", 1000), strings.Repeat("b", 1000), strings.Repeat("c", 1000), strings.Repeat("d", 1000), strings.Repeat("e", 1000)}, reason: attrDropSize},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cands, drops := mapAttrs(map[string]any{"department": tc.value}, []string{"department"})
			if tc.reason != "" {
				assert.Empty(t, cands)
				assert.Equal(t, []attrDrop{tc.reason}, drops)
				return
			}
			assert.Empty(t, drops)
			assert.Equal(t, []attrCandidate{{name: "department", values: tc.want}}, cands)
		})
	}
}

func TestMapAttrs_Allowlist(t *testing.T) {
	t.Parallel()

	session := map[string]any{
		"department": "finance",
		"country":    []any{"CZ", "SK"},
		"unlisted":   "secret",
		"email":      "someone@example.com",
		"sub":        "user-1",
		"address":    float64(1), // wrong type
	}

	t.Run("only the allowlisted claims, in the order of the allowlist", func(t *testing.T) {
		t.Parallel()
		cands, drops := mapAttrs(session, []string{"country", "department", "missing", "address"})
		assert.Equal(t, []attrCandidate{
			{name: "country", values: []string{"CZ", "SK"}},
			{name: "department", values: []string{"finance"}},
		}, cands)
		assert.Equal(t, []attrDrop{attrDropType}, drops, "a claim the IdP did not send is not a drop")
	})

	t.Run("no allowlist, no attributes", func(t *testing.T) {
		t.Parallel()
		cands, drops := mapAttrs(session, nil)
		assert.Empty(t, cands)
		assert.Empty(t, drops)
	})

	t.Run("no session claims", func(t *testing.T) {
		t.Parallel()
		cands, drops := mapAttrs(nil, []string{"department"})
		assert.Empty(t, cands)
		assert.Empty(t, drops)
	})
}

func TestFitAttrs(t *testing.T) {
	t.Parallel()

	// A token without groups: header ~60 B and payload ~450 B (apps-proxy signs about 0.8 KB).
	const headerLen, payloadLen = 60, 450

	cand := func(name, value string, n int) attrCandidate {
		values := make([]string, n)
		for i := range values {
			values[i] = value + strings.Repeat("v", i)
		}
		return attrCandidate{name: name, values: values}
	}

	t.Run("everything fits", func(t *testing.T) {
		t.Parallel()
		attrs, dropped := fitAttrs([]attrCandidate{cand("a", "x", 1), cand("b", "y", 2)}, headerLen, payloadLen)
		assert.Equal(t, map[string][]string{"a": {"x"}, "b": {"y", "yv"}}, attrs)
		assert.Zero(t, dropped)
		require.NoError(t, sdk.ValidateAttrs(attrs))
	})

	t.Run("nothing to fit", func(t *testing.T) {
		t.Parallel()
		attrs, dropped := fitAttrs(nil, headerLen, payloadLen)
		assert.Nil(t, attrs)
		assert.Zero(t, dropped)
	})

	t.Run("the whole attrs claim is capped at 4 KB, a later claim that fits is still taken", func(t *testing.T) {
		t.Parallel()
		big1 := attrCandidate{name: "big1", values: []string{strings.Repeat("a", 1024), strings.Repeat("b", 1024)}}
		big2 := attrCandidate{name: "big2", values: []string{strings.Repeat("c", 1024), strings.Repeat("d", 1024)}}
		big3 := attrCandidate{name: "big3", values: []string{strings.Repeat("e", 1024)}}
		small := attrCandidate{name: "small", values: []string{"s"}}

		attrs, dropped := fitAttrs([]attrCandidate{big1, big2, big3, small}, headerLen, payloadLen)

		assert.Equal(t, map[string][]string{"big1": big1.values, "big3": big3.values, "small": small.values}, attrs)
		assert.Equal(t, 1, dropped)
		assert.LessOrEqual(t, sdk.AttrsBytes(attrs), sdk.MaxAttrBytes)
	})

	t.Run("the largest groups leave no room: attrs are dropped first and the token stays within the budget", func(t *testing.T) {
		t.Parallel()
		// 4096 B of groups: the payload is about 0.45 KB + 4.6 KB of JSON, the token about 6.9 KB without attrs.
		const withGroups = 450 + 4600
		a := attrCandidate{name: "a", values: []string{strings.Repeat("w", 250), strings.Repeat("x", 250), strings.Repeat("y", 250), strings.Repeat("z", 250)}}
		b := attrCandidate{name: "b", values: []string{"b"}}
		require.LessOrEqual(t, sdk.AttrsBytes(map[string][]string{"a": a.values}), sdk.MaxAttrBytes, "a fits the cap of attrs, only the budget of the token stops it")

		attrs, dropped := fitAttrs([]attrCandidate{a, b}, headerLen, withGroups)

		assert.Equal(t, map[string][]string{"b": {"b"}}, attrs)
		assert.Equal(t, 1, dropped)
		total := encodedTokenLen(headerLen, withGroups+len(`,"attrs":`)+sdk.AttrsBytes(attrs))
		assert.LessOrEqual(t, total, maxEncodedTokenBytes)
	})

	t.Run("a payload that already fills the budget leaves no attrs", func(t *testing.T) {
		t.Parallel()
		attrs, dropped := fitAttrs([]attrCandidate{cand("a", "x", 1), cand("b", "y", 1)}, headerLen, 6000)
		assert.Empty(t, attrs)
		assert.Equal(t, 2, dropped)
	})

	t.Run("deterministic: the same input gives the same bytes", func(t *testing.T) {
		t.Parallel()
		cands := []attrCandidate{cand("z", "x", 4), cand("a", "y", 3), cand("m", strings.Repeat("q", 250), 2), cand("b", "w", 1), cand("c", strings.Repeat("r", 250), 3)}
		first, firstDropped := fitAttrs(cands, headerLen, 2500)
		want, err := json.Marshal(first)
		require.NoError(t, err)
		for range 200 {
			got, dropped := fitAttrs(cands, headerLen, 2500)
			raw, err := json.Marshal(got)
			require.NoError(t, err)
			require.Equal(t, string(want), string(raw))
			require.Equal(t, firstDropped, dropped)
		}
	})
}

func TestEncodedTokenLen(t *testing.T) {
	t.Parallel()

	// header, payload and the 64 B ES256 signature as unpadded base64url, plus two dots.
	assert.Equal(t, 4+1+4+1+86, encodedTokenLen(3, 3))
	assert.Equal(t, 6+1+7+1+86, encodedTokenLen(4, 5))
	assert.Equal(t, 7+1+8+1+86, encodedTokenLen(5, 6))
}

func toStrings(in []any) []string {
	out := make([]string, len(in))
	for i, v := range in {
		out[i], _ = v.(string)
	}
	return out
}
