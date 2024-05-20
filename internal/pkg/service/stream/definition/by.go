package definition

import (
	"strconv"

	"github.com/keboola/go-client/pkg/keboola"
)

const (
	BySystem ByType = "system"
	ByUser   ByType = "user"
)

type ByType string

// By describes actor of an operation.
type By struct {
	Type      ByType `json:"type" validate:"required,oneof=system user"`
	TokenID   string `json:"tokenId,omitempty"`
	TokenDesc string `json:"tokenDesc,omitempty"`
	UserID    string `json:"userId,omitempty"`
	UserName  string `json:"userName,omitempty"`
}

func (v ByType) String() string {
	return string(v)
}

func ByFromToken(token keboola.Token) By {
	v := By{
		Type:      ByUser,
		TokenID:   token.ID,
		TokenDesc: token.Description,
	}

	if token.Admin != nil {
		v.UserName = token.Admin.Name
		v.UserID = strconv.Itoa(token.Admin.ID)
	}

	return v
}
