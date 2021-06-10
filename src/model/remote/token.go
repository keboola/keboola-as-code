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

func (t *Token) ProjectId() int {
	return t.Owner.Id
}

func (t *Token) ProjectName() string {
	return t.Owner.Name
}
