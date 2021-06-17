package model

import (
	"fmt"
	"keboola-as-code/src/json"
	"sort"
	"strconv"
)

// Token https://keboola.docs.apiary.io/#reference/tokens-and-permissions/token-verification/token-verification
type Token struct {
	Id       string     `json:"id"`
	Token    string     `json:"token"`
	IsMaster bool       `json:"isMasterToken"`
	Owner    TokenOwner `json:"owner"`
}

type TokenOwner struct {
	Id   int    `json:"id"`
	Name string `json:"name"`
}

// Branch https://keboola.docs.apiary.io/#reference/development-branches/branches/list-branches
type Branch struct {
	Id          int    `json:"id" validate:"required"`
	Name        string `json:"name" validate:"required"`
	Description string `json:"description"`
	IsDefault   bool   `json:"isDefault"`
}

// Component https://keboola.docs.apiary.io/#reference/components-and-configurations/get-development-branch-components/get-development-branch-components
type Component struct {
	BranchId  int                    `json:"branchId" validate:"required"` // not present in API response, must be set manually
	Id        string                 `json:"id" validate:"required"`
	Type      string                 `json:"type" validate:"required"`
	Name      string                 `json:"name" validate:"required"`
	Configs   []*Config              `json:"configurations" validate:"required"`
	Schema    map[string]interface{} `json:"configurationSchema,omitempty"`
	SchemaRow map[string]interface{} `json:"configurationRowSchema,omitempty"`
}

// Config https://keboola.docs.apiary.io/#reference/components-and-configurations/component-configurations/list-configurations
type Config struct {
	BranchId          int                    `json:"branchId" validate:"required"`    // not present in API response, must be set manually
	ComponentId       string                 `json:"componentId" validate:"required"` // not present in API response, must be set manually
	Id                string                 `json:"id" validate:"required"`
	Name              string                 `json:"name" validate:"required"`
	Description       string                 `json:"description"`
	ChangeDescription string                 `json:"changeDescription"`
	Config            map[string]interface{} `json:"configuration" validate:"required"`
	Rows              []*ConfigRow           `json:"rows"`
}

// ConfigRow https://keboola.docs.apiary.io/#reference/components-and-configurations/component-configurations/list-configurations
type ConfigRow struct {
	BranchId          int                    `json:"branchId" validate:"required"`    // not present in API response, must be set manually
	ComponentId       string                 `json:"componentId" validate:"required"` // not present in API response, must be set manually
	ConfigId          string                 `json:"configId" validate:"required"`    // not present in API response, must be set manually
	Id                string                 `json:"id" validate:"required"`
	Name              string                 `json:"name" validate:"required"`
	Description       string                 `json:"description"`
	ChangeDescription string                 `json:"changeDescription"`
	IsDisabled        bool                   `json:"isDisabled"`
	Config            map[string]interface{} `json:"configuration" validate:"required"`
}

// Job - Storage API job
type Job struct {
	Id      int                    `json:"id" validate:"required"`
	Status  string                 `json:"status" validate:"required"`
	Url     string                 `json:"url" validate:"required"`
	Results map[string]interface{} `json:"results"`
}

// Event https://keboola.docs.apiary.io/#reference/events/events/create-event
type Event struct {
	Id string `json:"id"`
}

func (t *Token) ProjectId() int {
	return t.Owner.Id
}

func (t *Token) ProjectName() string {
	return t.Owner.Name
}

func (c *Config) SortRows() {
	sort.SliceStable(c.Rows, func(i, j int) bool {
		return c.Rows[i].Name < c.Rows[j].Name
	})
}

func (c *Config) ToApiValues() (map[string]string, error) {
	// Encode config
	configJson, err := json.Encode(c.Config, false)
	if err != nil {
		return nil, fmt.Errorf(`cannot JSON encode config configuration: %s`, err)
	}

	return map[string]string{
		"name":              c.Name,
		"description":       c.Description,
		"changeDescription": c.ChangeDescription,
		"configuration":     string(configJson),
	}, nil
}

func (r *ConfigRow) ToApiValues() (map[string]string, error) {
	// Encode config
	configJson, err := json.Encode(r.Config, false)
	if err != nil {
		return nil, fmt.Errorf(`cannot JSON encode config configuration: %s`, err)
	}

	return map[string]string{
		"name":              r.Name,
		"description":       r.Description,
		"changeDescription": r.ChangeDescription,
		"isDisabled":        strconv.FormatBool(r.IsDisabled),
		"configuration":     string(configJson),
	}, nil
}

func (b *Branch) UniqId() string {
	return fmt.Sprintf("%d", b.Id)
}

func (c *Component) UniqId() string {
	return fmt.Sprintf("%d_%s", c.BranchId, c.Id)
}

func (c *Config) UniqId() string {
	return fmt.Sprintf("%d_%s_%s", c.BranchId, c.ComponentId, c.Id)
}

func (r *ConfigRow) UniqId() string {
	return fmt.Sprintf("%d_%s__%s_%s", r.BranchId, r.ComponentId, r.ConfigId, r.Id)
}
