package api

import (
	"encoding/json"
	"reflect"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	ProviderTypeOIDC = ProviderType("oidc")
)

// ProviderID is unique identifier of the authentication provider inside a data app.
type ProviderID string

// ProviderType - each provider type have different settings.
type ProviderType string

// Providers type is a collection of authentication providers with different types.
type Providers []Provider

// Provider is common interface for all authentication providers.
type Provider interface {
	ID() ProviderID
	Name() string
	Type() ProviderType
}

// ProviderInfo is common information about authentication provider.
type ProviderInfo struct {
	ID   ProviderID   `json:"id"`
	Name string       `json:"name"`
	Type ProviderType `json:"type"`
}

type BaseProvider struct {
	ProviderInfo
}

func newProviderFromType(t ProviderType) (Provider, error) {
	switch t {
	case ProviderTypeOIDC:
		return OIDCProvider{}, nil
	default:
		return nil, errors.Errorf(`unexpected type of data app auth provider "%v"`, t)
	}
}

func (v ProviderID) String() string {
	return string(v)
}

func (v BaseProvider) ID() ProviderID {
	return v.ProviderInfo.ID
}

func (v BaseProvider) Name() string {
	// Use provider id as fallback until name is added to Sandboxes API
	if v.ProviderInfo.Name == "" {
		return v.ProviderInfo.ID.String()
	}
	return v.ProviderInfo.Name
}

func (v BaseProvider) Type() ProviderType {
	return v.ProviderInfo.Type
}

// UnmarshalJSON implements detection of the provider struct using the "type" field.
func (v *Providers) UnmarshalJSON(b []byte) error {
	*v = nil

	var items []json.RawMessage
	if err := json.Unmarshal(b, &items); err != nil {
		return err
	}

	for _, item := range items {
		// Detect provider type
		t := struct {
			Type ProviderType `json:"type"`
		}{}
		if err := json.Unmarshal(item, &t); err != nil {
			return err
		}

		// Unmarshal data to the provider struct
		itemValue, err := newProviderFromType(t.Type)
		if err != nil {
			return err
		}
		itemPtr := reflect.New(reflect.TypeOf(itemValue))
		itemPtr.Elem().Set(reflect.ValueOf(itemValue))
		if err = json.Unmarshal(item, itemPtr.Interface()); err != nil {
			return err
		}

		// Append the provider struct to the slice
		*v = append(*v, itemPtr.Elem().Interface().(Provider))
	}

	return nil
}
