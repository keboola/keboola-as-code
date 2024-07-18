package column

const (
	ColumnUUIDType Type = "uuid"
)

type UUID struct {
	Name       string `json:"name" validate:"required"`
	PrimaryKey bool   `json:"primaryKey,omitempty"`
}

func (v UUID) ColumnType() Type {
	return ColumnUUIDType
}

func (v UUID) ColumnName() string {
	return v.Name
}

func (v UUID) IsPrimaryKey() bool {
	return v.PrimaryKey
}
