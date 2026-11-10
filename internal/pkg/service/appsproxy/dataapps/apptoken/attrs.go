package apptoken

import (
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"

	sdk "github.com/keboola/keboola-sdk-go/v2/pkg/apptoken"
)

const (
	// maxEncodedTokenBytes is the budget of the whole signed token. Many app servers refuse a header line over 8190 B, and the
	// verifiers parse at most 8 KiB, so the token stays under 8000 B and the attributes give way to the groups.
	maxEncodedTokenBytes = 8000
	// encodedSignatureLen is the length of the 64 B ES256 signature as unpadded base64url.
	encodedSignatureLen = 86
	// attrsKeyLen is what the `attrs` claim adds to the payload around its own JSON: `,"attrs":`.
	attrsKeyLen = len(`,"attrs":`)
)

// attrDrop is the reason why a claim of the allowlist does not become an attribute. It is the label of the metric, so it is a
// fixed set that never names a claim or holds a value.
type attrDrop string

const (
	// attrDropType: the value is not a non-empty string or a list of non-empty strings, or it has a character that is not allowed.
	attrDropType attrDrop = "type"
	// attrDropSize: the value has too many values, too long a value, or is larger than the whole `attrs` claim may be.
	attrDropSize attrDrop = "size"
	// attrDropBudget: the claim is valid, but it does not fit the `attrs` cap or the budget of the token.
	attrDropBudget attrDrop = "budget"
)

// attrCandidate is a claim of the allowlist that has a usable value, before the budget of the token is applied.
type attrCandidate struct {
	name   string
	values []string
}

// mapAttrs takes the claims of the allowlist, in its order, from the claims of the login session. A claim that the identity
// provider did not send is not an attribute and not a drop. A claim with a value that cannot be one is dropped, which makes the
// attribute unknown, never empty: a non-empty string is one value, a list of non-empty strings is a list of values with the
// duplicates removed, and nothing else is accepted (the value is never converted).
func mapAttrs(claims map[string]any, allowlist []string) ([]attrCandidate, []attrDrop) {
	var candidates []attrCandidate
	var drops []attrDrop
	for _, name := range allowlist {
		raw, found := claims[name]
		if !found {
			continue
		}
		values, reason := mapAttrValue(raw)
		if reason == "" && entryLen(attrCandidate{name: name, values: values})+2 > sdk.MaxAttrBytes {
			reason = attrDropSize
		}
		if reason != "" {
			drops = append(drops, reason)
			continue
		}
		candidates = append(candidates, attrCandidate{name: name, values: values})
	}

	return candidates, drops
}

func mapAttrValue(raw any) ([]string, attrDrop) {
	var values []string
	switch v := raw.(type) {
	case string:
		values = []string{v}
	case []string:
		values = v
	case []any:
		values = make([]string, 0, len(v))
		for _, item := range v {
			s, ok := item.(string)
			if !ok {
				return nil, attrDropType
			}
			values = append(values, s)
		}
	default:
		return nil, attrDropType
	}

	if len(values) == 0 {
		return nil, attrDropType
	}
	for _, value := range values {
		if value == "" || !utf8.ValidString(value) || strings.ContainsFunc(value, unicode.IsControl) {
			return nil, attrDropType
		}
	}

	values = dedupe(values)
	if len(values) > sdk.MaxAttrValues {
		return nil, attrDropSize
	}
	for _, value := range values {
		if utf8.RuneCountInString(value) > sdk.MaxAttrValueChars {
			return nil, attrDropSize
		}
	}

	return values, ""
}

// dedupe removes the repeated values and keeps the order of the first ones.
func dedupe(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		if !slices.Contains(out, value) {
			out = append(out, value)
		}
	}

	return out
}

// fitAttrs takes the candidates in order, and keeps a candidate only while the `attrs` claim stays within sdk.MaxAttrBytes and the
// signed token within maxEncodedTokenBytes. headerLen and payloadLen are the JSON sizes of the JOSE header and of the payload
// without `attrs`. A candidate that does not fit is dropped (its attribute is unknown) and the next ones are still tried, so the
// result depends only on the order of the allowlist. It returns nil when nothing fits, and the number of the candidates dropped.
func fitAttrs(candidates []attrCandidate, headerLen, payloadLen int) (map[string][]string, int) {
	attrs := make(map[string][]string, len(candidates))
	size := 2 // the braces of an empty object
	dropped := 0
	for _, candidate := range candidates {
		entry := entryLen(candidate)
		next := size + entry
		if len(attrs) > 0 {
			next++ // the comma between two entries
		}
		if next > sdk.MaxAttrBytes || encodedTokenLen(headerLen, payloadLen+attrsKeyLen+next) > maxEncodedTokenBytes {
			dropped++
			continue
		}
		attrs[candidate.name] = candidate.values
		size = next
	}
	if len(attrs) == 0 {
		return nil, dropped
	}

	return attrs, dropped
}

// entryLen is the size of `"name":["value",...]` as raw JSON, the way sdk.AttrsBytes counts it (the braces of the object not included).
func entryLen(candidate attrCandidate) int {
	return sdk.AttrsBytes(map[string][]string{candidate.name: candidate.values}) - len(`{}`)
}

// encodedTokenLen is the length of a signed token whose header and payload have the given JSON sizes: both as unpadded base64url,
// the signature, and two dots.
func encodedTokenLen(headerLen, payloadLen int) int {
	return base64Len(headerLen) + 1 + base64Len(payloadLen) + 1 + encodedSignatureLen
}

// base64Len is the length of n bytes as unpadded base64url.
func base64Len(n int) int {
	return (n*4 + 2) / 3
}
