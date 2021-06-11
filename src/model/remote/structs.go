package remote

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
	Id        int    `json:"id"`
	Name      string `json:"name"`
	IsDefault bool   `json:"isDefault"`
}

// Configuration https://keboola.docs.apiary.io/#reference/components-and-configurations/component-configurations/list-configurations
type Configuration struct {
	BranchId      int                    `json:"branchId"`    // not present in API response, must be set manually
	ComponentId   string                 `json:"componentId"` // not present in API response, must be set manually
	Id            int                    `json:"id"`
	Name          string                 `json:"name"`
	Configuration map[string]interface{} `json:"configuration"`
	Rows          []Row                  `json:"rows"`
}

// Row https://keboola.docs.apiary.io/#reference/components-and-configurations/component-configurations/list-configurations
type Row struct {
	Id            int                    `json:"id"`
	Name          string                 `json:"name"`
	Configuration map[string]interface{} `json:"configuration"`
}

func (t *Token) ProjectId() int {
	return t.Owner.Id
}

func (t *Token) ProjectName() string {
	return t.Owner.Name
}
