package column

const (
	ColumnPathType Type = "path"
)

type Path struct {
	Name         string  `json:"name" validate:"required"`
	PrimaryKey   bool    `json:"primaryKey,omitempty"`
	Path         string  `json:"path" validate:"required"`
	RawString    bool    `json:"rawString,omitempty"`
	DefaultValue *string `json:"defaultValue,omitempty"`
}

func (v Path) ColumnType() Type {
	return ColumnPathType
}

func (v Path) ColumnName() string {
	return v.Name
}

func (v Path) IsPrimaryKey() bool {
	return v.PrimaryKey
}

func (v Path) ColumnPath() string {
	return v.Path
}

func (v Path) ReturnsRawString() bool {
	return v.RawString
}

func (v Path) GetDefaultValue() *string {
	return v.DefaultValue
}
