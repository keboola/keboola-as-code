package column

const (
	TimeFormat              = "2006-01-02T15:04:05.000Z"
	ColumnDatetimeType Type = "datetime"
)

type Datetime struct {
	Name       string `json:"name" validate:"required"`
	PrimaryKey bool   `json:"primaryKey,omitempty"`
}

func (v Datetime) ColumnType() Type {
	return ColumnDatetimeType
}

func (v Datetime) ColumnName() string {
	return v.Name
}

func (v Datetime) IsPrimaryKey() bool {
	return v.PrimaryKey
}
