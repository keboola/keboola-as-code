package primarykey

import (
	"database/sql/driver"
)

// KeyPlaceholder implements field.ValueScanner.
type KeyPlaceholder struct{}

func (v *KeyPlaceholder) String() string {
	return ""
}

func (v *KeyPlaceholder) Value() (driver.Value, error) {
	return nil, nil
}

func (v *KeyPlaceholder) Scan(any) error {
	return nil
}

func (v *KeyPlaceholder) Validate() error {
	return nil
}
