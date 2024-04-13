package api

const (
	RulePathPrefix = RuleType("pathPrefix")
)

// RuleType specifies URLs matching mechanism for Rule.Value.
type RuleType string

// Rule specifies which authentication Providers should be used for matched data app URLs.
type Rule struct {
	Type         RuleType     `json:"type"`
	Value        string       `json:"value"`
	Auth         []ProviderID `json:"auth"`
	AuthRequired *bool        `json:"authRequired"`
}
