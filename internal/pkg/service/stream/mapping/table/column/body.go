package column

const (
	ColumnBodyType Type = "body"
)

type Body struct {
	Name       string `json:"name" validate:"required"`
	PrimaryKey bool   `json:"primaryKey,omitempty"`
}

func (v Body) ColumnType() Type {
	return ColumnBodyType
}

func (v Body) ColumnName() string {
	return v.Name
}

func (v Body) IsPrimaryKey() bool {
	return v.PrimaryKey
}
