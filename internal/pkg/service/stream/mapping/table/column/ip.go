package column

const (
	ColumnIPType Type = "ip"
)

type IP struct {
	Name       string `json:"name" validate:"required"`
	PrimaryKey bool   `json:"primaryKey,omitempty"`
}

func (v IP) ColumnType() Type {
	return ColumnIPType
}

func (v IP) ColumnName() string {
	return v.Name
}

func (v IP) IsPrimaryKey() bool {
	return v.PrimaryKey
}
