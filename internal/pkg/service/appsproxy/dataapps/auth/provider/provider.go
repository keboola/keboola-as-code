// Package provider providers configuration structures for different authentication providers.
package provider

import (
	"encoding/json"
	"reflect"
)

const (
	TypeOIDC = Type("oidc")
)

// ID is unique identifier of the authentication provider inside a data app.
type ID string

// Type - each provider type have different settings.
type Type string

// Providers type is a collection of authentication providers with different types.
type Providers []Provider

// Provider is common interface for all authentication providers.
type Provider interface {
	ID() ID
	Name() string
	Type() Type
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
			Type Type `json:"type"`
		}{}
		if err := json.Unmarshal(item, &t); err != nil {
			return err
		}

		// Unmarshal data to the provider struct
		itemValue, err := t.Type.new()
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
