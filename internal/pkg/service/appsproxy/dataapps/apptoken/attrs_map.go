package apptoken

import (
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/keboola/go-utils/pkg/orderedmap"
	sdk "github.com/keboola/keboola-sdk-go/v2/pkg/apptoken"
)

// The per-claim limits (names, values per claim, characters per value, size of attrs) are the contract of the token and live in
// keboola-sdk-go, pkg/apptoken: they are imported here and never redefined. What is local to apps-proxy is the budget of the whole
// token, see attrs_budget.go.

// The reasons why a claim of the allowlist does not become an attribute.
const (
	// attrDropType: the value is not a non-empty string or a list of non-empty strings, or it has a character that is not allowed.
	attrDropType attrDrop = "type"
	// attrDropSize: the value has too many values, too long a value, or is larger than the whole `attrs` claim may be.
	attrDropSize attrDrop = "size"
	// attrDropBudget: the claim is valid, but it does not fit the `attrs` cap or the budget of the token.
	attrDropBudget attrDrop = "budget"
)

// attrDrop is a reason of the drop of a claim. It is the label of the metric, so it is a fixed set that never names a claim or holds
// a value.
type attrDrop string

// attrCandidate is a claim of the allowlist that has a usable value, before the budget of the token is applied.
type attrCandidate struct {
	name   string
	values []string
}

// mapAttrs takes the claims of the allowlist, in its order, from the claims of the login session. A claim that the identity
// provider did not send is not an attribute and not a drop. A claim with a value that cannot be one is dropped, which makes the
// attribute unknown, never empty. See mapAttrValue.
func mapAttrs(claims map[string]any, allowlist []string) ([]attrCandidate, []attrDrop) {
	var candidates []attrCandidate
	var drops []attrDrop
	for _, name := range allowlist {
		raw, found := claims[name]
		if !found {
			continue
		}
		values, reason := mapAttrValue(raw)
		if reason == "" && entryLen(attrCandidate{name: name, values: values})+len("{}") > sdk.MaxAttrBytes {
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

// mapAttrValue returns the values of a claim: a non-empty string is one value, a list of non-empty strings is a list of values
// (sorted, without repeats: the order of the values of one attribute does not matter to a rule, which matches any of them, and a
// canonical order makes the token the same whatever order the IdP sends). Nothing else is accepted and nothing is converted: a
// number or a bool is not turned into a string, which is why the claim getters of oauth2-proxy are not used.
//
// The checks of a value mirror sdk.ValidateAttrs (an empty value, invalid UTF-8, a control character, more than sdk.MaxAttrValues
// values, more than sdk.MaxAttrValueChars characters). They are repeated here because the reason has to be told apart (type or
// size) for the metric, and the SDK reports only that the claim is invalid.
func mapAttrValue(raw any) ([]string, attrDrop) {
	var values []string
	if s, isString := raw.(string); isString {
		values = []string{s}
	} else if list, err := orderedmap.ToStringSlice(raw); err == nil {
		values = list
	}
	if len(values) == 0 {
		return nil, attrDropType
	}
	for _, value := range values {
		if value == "" || !utf8.ValidString(value) || strings.ContainsFunc(value, unicode.IsControl) {
			return nil, attrDropType
		}
	}

	values = slices.Compact(slices.Sorted(slices.Values(values)))
	if len(values) > sdk.MaxAttrValues || slices.ContainsFunc(values, func(v string) bool { return utf8.RuneCountInString(v) > sdk.MaxAttrValueChars }) {
		return nil, attrDropSize
	}

	return values, ""
}
