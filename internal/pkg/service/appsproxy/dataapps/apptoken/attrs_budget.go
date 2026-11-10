package apptoken

import (
	"encoding/base64"

	sdk "github.com/keboola/keboola-sdk-go/v2/pkg/apptoken"
)

// These values are local to apps-proxy, they are not part of the contract of the token: many app servers refuse a header line over
// 8190 B, and a verifier parses at most 8 KiB, so the whole signed token stays under 8000 B and the attributes give way to the groups.
const (
	maxEncodedTokenBytes = 8000
	// es256SignatureBytes is the length of an ES256 signature (r and s of P-256, 32 B each).
	es256SignatureBytes = 64
	// attrsKeyLen is what the `attrs` claim adds to the payload around its own JSON: `,"attrs":`.
	attrsKeyLen = len(`,"attrs":`)
)

// fitAttrs takes the candidates in order, and keeps a candidate only while the `attrs` claim stays within sdk.MaxAttrBytes and the
// signed token within maxEncodedTokenBytes. headerLen and payloadLen are the JSON sizes of the JOSE header and of the payload
// without `attrs`. A candidate that does not fit is dropped (its attribute is unknown) and the next ones are still tried, so the
// result depends only on the order of the allowlist. It returns nil when nothing fits, and the number of the candidates dropped.
func fitAttrs(candidates []attrCandidate, headerLen, payloadLen int) (map[string][]string, int) {
	attrs := make(map[string][]string, len(candidates))
	size := len("{}")
	dropped := 0
	for _, candidate := range candidates {
		next := size + entryLen(candidate)
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
	return sdk.AttrsBytes(map[string][]string{candidate.name: candidate.values}) - len("{}")
}

// encodedTokenLen is the length of a signed token whose header and payload have the given JSON sizes: both and the signature as
// unpadded base64url, and two dots.
func encodedTokenLen(headerLen, payloadLen int) int {
	enc := base64.RawURLEncoding
	return enc.EncodedLen(headerLen) + 1 + enc.EncodedLen(payloadLen) + 1 + enc.EncodedLen(es256SignatureBytes)
}
