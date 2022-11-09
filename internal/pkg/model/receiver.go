package model

type Receiver struct {
	ID        string `json:"receiverId"`
	ProjectID string `json:"projectId"`
	Name      string `json:"name"`
	Secret    string `json:"secret"`
}
