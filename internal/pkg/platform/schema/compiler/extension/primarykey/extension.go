package primarykey

import (
	"strings"

	"entgo.io/ent/entc"
	"entgo.io/ent/entc/gen"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
)

type Extension struct {
	entc.DefaultExtension
}

func (Extension) Hooks() []gen.Hook {
	return []gen.Hook{
		func(next gen.Generator) gen.Generator {
			return gen.GenerateFunc(func(g *gen.Graph) error {
				// Replace KeyPlaceholder struct with generated keys structs.
				for _, n := range g.Nodes {
					if asMap, ok := n.ID.Annotations[pkAnnotationName]; ok {
						// asMap is PKAnnotation type serialized to a map
						pkAnnotation := PKAnnotation{}
						if err := json.ConvertByJSON(asMap, &pkAnnotation); err != nil {
							return err
						}

						// Replace the KeyPlaceholder with the generated key
						structName := keyStructName(n.Name)
						structNameWithPkg := keyPgkName + "." + structName
						pkgPath := n.Config.Package + "/" + keyPgkName
						n.ID.Type.Ident = structNameWithPkg
						n.ID.Type.PkgPath = pkgPath
						n.ID.Type.PkgName = keyPgkName
						n.ID.Type.RType.Name = structName
						n.ID.Type.RType.Ident = structNameWithPkg
						n.ID.Type.RType.PkgPath = pkgPath
					}

					for _, index := range n.Indexes {
						if _, ok := index.Annotations[pkComposedIndexAnnotationName]; ok {
							index.Name = strings.ToLower(n.Name) + "_pk_composed"
						}
						if _, ok := index.Annotations[pkFieldAnnotationName]; ok {
							index.Name = strings.ToLower(n.Name) + "_pk_field_" + strings.ToLower(index.Columns[0])
						}
					}
				}
				return next.Generate(g)
			})
		},
	}
}

func (Extension) Templates() []*gen.Template {
	return []*gen.Template{
		// Generate modified mutations, see customMutationTemplate docs.
		customMutationTemplate(),
	}
}
