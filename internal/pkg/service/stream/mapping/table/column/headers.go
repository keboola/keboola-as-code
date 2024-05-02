package column

const (
	columnHeadersType Type = "headers"
)

type Headers struct {
	Name       string `json:"name" validate:"required"`
	PrimaryKey bool   `json:"primaryKey,omitempty"`
}

func (v Headers) ColumnType() Type {
	return columnHeadersType
}

func (v Headers) ColumnName() string {
	return v.Name
}

func (v Headers) IsPrimaryKey() bool {
	return v.PrimaryKey
}
