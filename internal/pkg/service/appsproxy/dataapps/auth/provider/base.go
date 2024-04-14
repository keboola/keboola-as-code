package provider

import "github.com/keboola/keboola-as-code/internal/pkg/utils/errors"

// Base is common implementation of the Provider interface.
type Base struct {
	Info
}

// Info is common information about authentication provider.
type Info struct {
	ID   ID     `json:"id"`
	Name string `json:"name"`
	Type Type   `json:"type"`
}

// new creates a new empty provider of the type.
func (t Type) new() (Provider, error) {
	switch t {
	case TypeOIDC:
		return OIDC{}, nil
	default:
		return nil, errors.Errorf(`unexpected type of data app auth provider "%v"`, t)
	}
}

func (v ID) String() string {
	return string(v)
}

func (v Base) ID() ID {
	return v.Info.ID
}

func (v Base) Name() string {
	// Use provider id as fallback until name is added to Sandboxes API
	if v.Info.Name == "" {
		return v.Info.ID.String()
	}
	return v.Info.Name
}

func (v Base) Type() Type {
	return v.Info.Type
}
