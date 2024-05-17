package test

import "github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"

func ByUser() definition.By {
	return definition.By{
		Type:      definition.ByUser,
		TokenID:   "111",
		TokenDesc: "some.user@company.com",
		UserID:    "222",
		UserName:  "Some User",
	}
}
