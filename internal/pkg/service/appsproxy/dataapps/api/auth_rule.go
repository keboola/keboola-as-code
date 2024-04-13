package api

const (
	PathPrefix = RuleType("pathPrefix")
)

type RuleType string

type AuthRule struct {
	Type         RuleType `json:"type"`
	Value        string   `json:"value"`
	Auth         []string `json:"auth"`
	AuthRequired *bool    `json:"authRequired"`
}
