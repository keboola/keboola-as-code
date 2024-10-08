package column

const (
	TemplateLanguageJsonnet      = "jsonnet"
	ColumnTemplateType      Type = "template"
)

type Template struct {
	Name       string         `json:"name" validate:"required"`
	PrimaryKey bool           `json:"primaryKey,omitempty"`
	RawString  bool           `json:"rawString,omitempty"`
	Template   TemplateConfig `json:"template"`
}

type TemplateConfig struct {
	Language string `json:"language" validate:"required,oneof=jsonnet"`
	Content  string `json:"content" validate:"required,min=1,max=4096"`
}

func (v Template) ColumnType() Type {
	return ColumnTemplateType
}

func (v Template) ColumnName() string {
	return v.Name
}

func (v Template) IsPrimaryKey() bool {
	return v.PrimaryKey
}

func (v Template) ReturnsRawString() bool {
	return v.RawString
}
