package provider

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
