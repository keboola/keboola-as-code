package local

import (
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type PathsGenerator struct {
	*Manager
	rename    bool // rename=false -> path is generated only for new objects, rename=true -> all paths are re-generated
	toUpdate  []model.ObjectState
	processed map[string]bool // Key.String() -> true
	renamed   []model.RenamedPath
	invoked   bool
}

func (m *Manager) NewPathsGenerator(rename bool) *PathsGenerator {
	return &PathsGenerator{Manager: m, rename: rename, processed: make(map[string]bool)}
}

func (g *PathsGenerator) Add(objectState model.ObjectState) *PathsGenerator {
	if g.invoked {
		panic(errors.New(`PathsGenerator have already been invoked`))
	}
	g.toUpdate = append(g.toUpdate, objectState)
	return g
}

func (g *PathsGenerator) AddRenamed(path model.RenamedPath) {
	if g.invoked {
		panic(errors.New(`PathsGenerator have already been invoked`))
	}
	g.renamed = append(g.renamed, path)
}

func (g *PathsGenerator) RenameEnabled() bool {
	return g.rename
}

func (g *PathsGenerator) Invoke() error {
	if g.invoked {
		panic(errors.New(`PathsGenerator must be first invoked to get list of the renamed objects`))
	}

	errs := errors.NewMultiError()
	for _, objectState := range g.toUpdate {
		if err := g.doUpdate(objectState, nil); err != nil {
			errs.Append(err)
		}
	}

	g.invoked = true
	return errs.ErrorOrNil()
}

func (g *PathsGenerator) Renamed() []model.RenamedPath {
	if !g.invoked {
		panic(errors.New(`PathsGenerator must be first invoked to get list of the renamed objects`))
	}
	return g.renamed
}

func (g *PathsGenerator) doUpdate(objectState model.ObjectState, origin model.Key) error {
	// Is already processed?
	if g.processed[objectState.Key().String()] {
		return nil
	}

	// Detect cyclic relations
	if origin != nil && objectState.Key().String() == origin.String() {
		return errors.Errorf(`a cyclic relation was found when generating path to %s`, origin.Desc())
	}
	if origin == nil {
		origin = objectState.Key()
	}

	// Use remote state if the local state is not set
	object := objectState.LocalOrRemoteState()
	manifest := objectState.Manifest()

	// Store old path
	oldPath := objectState.Path()

	// Update parent path
	parentKey, err := object.ParentKey()
	if err != nil {
		return err
	} else if parentKey != nil {
		// Update parent path
		parent := g.state.MustGet(parentKey)
		if err := g.doUpdate(parent, origin); err != nil {
			return err
		}

		// Set new parent path
		manifest.SetParentPath(parent.Path())
	}

	// Replace already renamed parts of the old path
	renameFrom := oldPath
	for _, item := range g.renamed {
		if filesystem.IsFrom(renameFrom, item.RenameFrom) {
			if relPath, err := filesystem.Rel(item.RenameFrom, renameFrom); err == nil {
				renameFrom = filesystem.Join(item.NewPath, relPath)
			} else {
				return err
			}
		}
	}

	// Re-generate object path IF rename is enabled OR path is not set
	if objectState.GetRelativePath() == "" || g.rename {
		if err := g.regeneratePath(objectState, object, oldPath, renameFrom); err != nil {
			return err
		}
	}

	// Mark processed
	g.processed[objectState.Key().String()] = true

	return nil
}

func (g *PathsGenerator) regeneratePath(objectState model.ObjectState, object model.Object, oldPath, renameFrom string) error {
	switch v := objectState.(type) {
	case *model.BranchState:
		v.AbsPath = g.NamingGenerator().BranchPath(object.(*model.Branch))
	case *model.ConfigState:
		config := object.(*model.Config)
		if component, err := g.state.Components().GetOrErr(config.ComponentID); err == nil {
			v.AbsPath = g.NamingGenerator().ConfigPath(v.GetParentPath(), component, config)
		} else {
			return err
		}
	case *model.ConfigRowState:
		row := object.(*model.ConfigRow)
		if component, err := g.state.Components().GetOrErr(row.ComponentID); err == nil {
			v.AbsPath = g.NamingGenerator().ConfigRowPath(v.GetParentPath(), component, row)
		} else {
			return err
		}
	default:
		panic(errors.Errorf(`unexpect type "%T"`, objectState))
	}

	// Has been object renamed?
	newPath := objectState.Path()
	renamed := false
	if renameFrom != newPath {
		renamed = true
		g.AddRenamed(model.RenamedPath{ObjectState: objectState, OldPath: oldPath, RenameFrom: renameFrom, NewPath: newPath})
	}

	// Event
	event := model.OnObjectPathUpdateEvent{
		PathsGenerator: g,
		ObjectState:    objectState,
		Renamed:        renamed,
		OldPath:        oldPath,
		NewPath:        newPath,
	}
	if err := g.mapper.OnObjectPathUpdate(event); err != nil {
		return err
	}

	return nil
}
