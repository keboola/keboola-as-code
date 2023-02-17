package primarykey

import (
	"reflect"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

const (
	pkAnnotationName              = "PrimaryKey"
	pkFieldAnnotationName         = "PrimaryKeyField"
	pkComposedIndexAnnotationName = "PrimaryKeyComposedIndex"
	pkFieldIndexAnnotationName    = "PrimaryKeyFieldIndex"
)

// PKAnnotation for the schema ID field.
type PKAnnotation struct {
	Fields []PKFieldAnnotation
}

// PKFieldAnnotation for each generated field used in the PrimaryKey.
type PKFieldAnnotation struct {
	PublicName  string
	PrivateName string
	GoType      GoType
}

type PKComposedIndexAnnotation struct{}

type PKFieldIndexAnnotation struct{}

type GoType struct {
	Name    string
	Kind    reflect.Kind
	KindStr string
	PkgPath string
}

func (PKAnnotation) Name() string {
	return pkAnnotationName
}

func (PKFieldAnnotation) Name() string {
	return pkFieldAnnotationName
}

func (PKComposedIndexAnnotation) Name() string {
	return pkComposedIndexAnnotationName
}

func (PKFieldIndexAnnotation) Name() string {
	return pkFieldIndexAnnotationName
}

// annotation creates PKAnnotation for the ID field.
func (v *Mixin) annotation() (out PKAnnotation) {
	for _, f := range v.pkFields {
		out.Fields = append(out.Fields, f.annotation())
	}
	return out
}

// annotation creates PKFieldAnnotation the PK field.
func (f pkFieldConfig) annotation() PKFieldAnnotation {
	t := reflect.TypeOf(f.Type)
	var kindStr string
	switch t.Kind() {
	case reflect.Int:
		kindStr = "int"
	case reflect.String:
		kindStr = "string"
	default:
		panic(errors.Errorf(`unexpected field "%s" type "%s", expected int or string`, f.Name, t.String()))
	}

	return PKFieldAnnotation{
		PublicName:  strhelper.FirstUpper(f.Name),
		PrivateName: f.Name,
		GoType: GoType{
			Name:    t.String(),
			Kind:    t.Kind(),
			KindStr: kindStr,
			PkgPath: t.PkgPath(),
		},
	}
}
