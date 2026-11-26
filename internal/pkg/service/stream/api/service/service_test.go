package service

import (
	"testing"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/stretchr/testify/assert"
)

func TestCanWrite(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		token keboola.Token
		want  bool
	}{
		{name: "admin", token: keboola.Token{Admin: &keboola.TokenAdmin{Role: "admin"}}, want: true},
		{name: "share", token: keboola.Token{Admin: &keboola.TokenAdmin{Role: "share"}}, want: true},
		{name: "readOnly", token: keboola.Token{Admin: &keboola.TokenAdmin{Role: "readOnly"}}, want: false},
		{name: "guest", token: keboola.Token{Admin: &keboola.TokenAdmin{Role: "guest"}}, want: false},
		{name: "empty role", token: keboola.Token{Admin: &keboola.TokenAdmin{}}, want: false},
		{name: "not a master token", token: keboola.Token{}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, canWrite(tt.token))
		})
	}
}
