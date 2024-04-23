package column

const (
	IDPlaceholder      = "<<~~id~~>>"
	columnIDType  Type = "id"
)

type ID struct {
	Name       string `json:"name" validate:"required"`
	PrimaryKey bool   `json:"primaryKey,omitempty"`
}

func (v ID) ColumnType() Type {
	return columnIDType
}

func (v ID) ColumnName() string {
	return v.Name
}

func (v ID) IsPrimaryKey() bool {
	return v.PrimaryKey
}
