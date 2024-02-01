// nolint: tagliatelle // because tagliatelle forces camel case for json keys
package mockoidc

import (
	"encoding/json"

	"github.com/golang-jwt/jwt"
	"github.com/oauth2-proxy/mockoidc"
)

// MockUser is a custom implementation of mockoidc.User.
// The only change from the default implementation is that EmailVerified is *bool instead of bool.
// This is necessary in order to correctly test a user with an email_verified=false claim.
// With the default implementation false was removed due to omitempty.
type MockUser struct {
	Subject           string
	Email             string
	EmailVerified     *bool
	PreferredUsername string
	Phone             string
	Address           string
	Groups            []string
}

type mockUserinfo struct {
	Email             string   `json:"email,omitempty"`
	PreferredUsername string   `json:"preferred_username,omitempty"`
	Phone             string   `json:"phone_number,omitempty"`
	Address           string   `json:"address,omitempty"`
	Groups            []string `json:"groups,omitempty"`
}

func (u *MockUser) ID() string {
	return u.Subject
}

func (u *MockUser) Userinfo(scope []string) ([]byte, error) {
	user := u.scopedClone(scope)

	info := &mockUserinfo{
		Email:             user.Email,
		PreferredUsername: user.PreferredUsername,
		Phone:             user.Phone,
		Address:           user.Address,
		Groups:            user.Groups,
	}

	return json.Marshal(info)
}

type mockClaims struct {
	*mockoidc.IDTokenClaims
	Email             string   `json:"email,omitempty"`
	EmailVerified     *bool    `json:"email_verified,omitempty"`
	PreferredUsername string   `json:"preferred_username,omitempty"`
	Phone             string   `json:"phone_number,omitempty"`
	Address           string   `json:"address,omitempty"`
	Groups            []string `json:"groups,omitempty"`
}

func (u *MockUser) Claims(scope []string, claims *mockoidc.IDTokenClaims) (jwt.Claims, error) {
	user := u.scopedClone(scope)

	return &mockClaims{
		IDTokenClaims:     claims,
		Email:             user.Email,
		EmailVerified:     user.EmailVerified,
		PreferredUsername: user.PreferredUsername,
		Phone:             user.Phone,
		Address:           user.Address,
		Groups:            user.Groups,
	}, nil
}

func (u *MockUser) scopedClone(scopes []string) *MockUser {
	clone := &MockUser{
		Subject: u.Subject,
	}
	for _, scope := range scopes {
		switch scope {
		case "profile":
			clone.PreferredUsername = u.PreferredUsername
			clone.Address = u.Address
			clone.Phone = u.Phone
		case "email":
			clone.Email = u.Email
			clone.EmailVerified = u.EmailVerified
		case "groups":
			clone.Groups = append(make([]string, 0, len(u.Groups)), u.Groups...)
		}
	}
	return clone
}
