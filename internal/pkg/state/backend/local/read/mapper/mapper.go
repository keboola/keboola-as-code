package mapper

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
)

type OnObjectReadListener interface {
	OnObjectRead(ctx context.Context, readCtx ObjectReadContext) error
}

type AfterAllObjectsReadListener interface {
	AfterAllObjectsRead(ctx context.Context, allReadCtx AllObjectsReadContext) error
}

type ObjectReadContext interface {
	Object() model.Object
	Path() model.AbsPath
	FileLoader() model.FilesLoader
	Naming() *naming.Generator
	SetAnnotation(key string)
	GetAnnotation(key string) any
}

type AllObjectsReadContext interface {
	Objects() model.Object
}

//// ObjectReadContext - all items related to the object, when loading it from the filesystem.
//type ObjectReadContext struct {
//	ObjectManifest                        // manifest record, eg *ConfigManifest
//	Object         Object                 // object, eg. Config
//	Files          *model.FilesLoader     // eg. config.json, meta.json, description.md, ...
//	Annotations    map[string]interface{} // key/value pairs that can be used by to affect mappers behavior
//}
//
//type AllObjectsReadContext struct {
//}

type mapper struct {
	listeners []any
}

func (m *mapper) ProcessObjects(ctx context.Context, fileLoader FilesLoader) {
	// OnObjectRead
	for _, object := range objects {
		for _, listener := range m.listeners {
			if v, ok := listener.(OnObjectReadListener); ok {
				readCtx := ObjectReadContext{}
				if err := v.OnObjectRead(ctx, readCtx); err != nil {
					// TODO
				}
			}
		}
	}

	// AfterAllObjectsRead
	for _, listener := range m.listeners {
		if v, ok := listener.(AfterAllObjectsReadListener); ok {
			allReadContext := AllObjectsReadContext{}
			if err := v.AfterAllObjectsRead(ctx, allReadContext); err != nil {
				// TODO
			}
		}
	}
}
