package remote

// Branch https://keboola.docs.apiary.io/#reference/development-branches/branches/list-branches
type Branch struct {
	Id        int    `json:"id"`
	Name      string `json:"name"`
	IsDefault bool   `json:"isDefault"`
}
