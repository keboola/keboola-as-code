package provider

type Basic struct {
	Base
	Password string `json:"password"`
}

func (p *Basic) IsAuthorized(password string) bool {
	return p.Password == password
}
