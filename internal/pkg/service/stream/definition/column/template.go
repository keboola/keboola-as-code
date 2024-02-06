package column

import (
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/api/receive/jsonnet"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/api/receive/receivectx"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	TemplateLanguageJsonnet      = "jsonnet"
	columnTemplateType      Type = "template"
)

type Template struct {
	Name       string `json:"name" validate:"required"`
	PrimaryKey bool   `json:"primaryKey,omitempty"`
	Language   string `json:"language" validate:"required,oneof=jsonnet"`
	Content    string `json:"content" validate:"required,min=1,max=4096"`
}

func (v Template) ColumnType() Type {
	return columnTemplateType
}

func (v Template) ColumnName() string {
	return v.Name
}

func (v Template) IsPrimaryKey() bool {
	return v.PrimaryKey
}

func (v Template) CSVValue(reqCtx *receivectx.Context) (string, error) {
	if v.Language == TemplateLanguageJsonnet {
		res, err := jsonnet.Evaluate(reqCtx, v.Content)
		if err != nil {
			return "", err
		}
		return strings.TrimRight(res, "\n"), nil
	}
	return "", errors.Errorf(`unsupported language "%s", only "jsonnet" is supported`, v.Language)
}
