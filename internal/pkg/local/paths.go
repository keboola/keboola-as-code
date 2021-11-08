package local

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

type PathsGenerator struct {
	*Manager
	rename    bool            // rename=false -> path is generated only for new objects, rename=true -> all paths are re-generated
	processed map[string]bool // Key.String() -> true
	renamed   []renamedPath
}

type renamedPath struct {
	ObjectState model.ObjectState
	OldPath     string
	RenameFrom  string // old path with renamed parents dirs
	NewPath     string
}

func (m *Manager) NewPathsGenerator(rename bool) *PathsGenerator {
	return &PathsGenerator{Manager: m, rename: rename, processed: make(map[string]bool)}
}

func (g *PathsGenerator) Update(objectState model.ObjectState) error {
	return g.doUpdate(objectState, nil)
}

func (g *PathsGenerator) Renamed() []renamedPath {
	return g.renamed
}

func (g *PathsGenerator) doUpdate(objectState model.ObjectState, origin model.Key) error {
	// Is already processed?
	if g.processed[objectState.Key().String()] {
		return nil
	}

	// Detect cyclic relations
	if origin != nil && objectState.Key().String() == origin.String() {
		return fmt.Errorf(`a cyclic relation was found when generating path to %s`, origin.Desc())
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
	if objectState.GetObjectPath() == "" || g.rename {
		switch v := objectState.(type) {
		case *model.BranchState:
			v.PathInProject = g.Naming().BranchPath(object.(*model.Branch))
		case *model.ConfigState:
			config := object.(*model.Config)
			if component, err := g.state.Components().Get(config.ComponentKey()); err == nil {
				v.PathInProject = g.Naming().ConfigPath(v.GetParentPath(), component, config)
			} else {
				return err
			}
		case *model.ConfigRowState:
			row := object.(*model.ConfigRow)
			if component, err := g.state.Components().Get(row.ComponentKey()); err == nil {
				v.PathInProject = g.Naming().ConfigRowPath(v.GetParentPath(), component, row)
			} else {
				return err
			}
		default:
			panic(fmt.Errorf(`unexpect type "%T"`, objectState))
		}

		// Has been object renamed?
		newPath := objectState.Path()
		if renameFrom != newPath {
			g.renamed = append(g.renamed, renamedPath{ObjectState: objectState, OldPath: oldPath, RenameFrom: renameFrom, NewPath: newPath})
		}

		// Rename transformation blocks
		if v, ok := objectState.(*model.ConfigState); ok && v.HasLocalState() {
			for _, block := range v.Local.Blocks {
				g.updateBlockPath(v, block)
			}
		}
	}

	// Mark processed
	g.processed[objectState.Key().String()] = true

	return nil
}

func (g *PathsGenerator) updateBlockPath(parent *model.ConfigState, block *model.Block) {
	// Update parent path
	oldPath := block.Path()
	blocksDir := g.Naming().BlocksDir(parent.Path())
	block.SetParentPath(blocksDir)

	// Re-generate object path IF rename is enabled OR path is not set
	if block.ObjectPath == "" || g.rename {
		renameFrom := block.Path()
		block.PathInProject = g.Naming().BlockPath(block.GetParentPath(), block)

		// Has been block renamed?
		newPath := block.Path()
		if renameFrom != newPath {
			g.renamed = append(g.renamed, renamedPath{ObjectState: parent, OldPath: oldPath, RenameFrom: renameFrom, NewPath: newPath})
		}
	}

	// Process codes
	for _, code := range block.Codes {
		g.updateCodePath(parent, block, code)
	}
}

func (g *PathsGenerator) updateCodePath(parent *model.ConfigState, block *model.Block, code *model.Code) {
	// Update parent path
	oldPath := block.Path()
	oldPathCodeFile := g.Naming().CodeFilePath(code)
	code.SetParentPath(block.Path())

	// Re-generate object path IF rename is enabled OR path is not set
	if block.ObjectPath == "" || g.rename {
		renameFrom := code.Path()
		code.PathInProject = g.Naming().CodePath(code.GetParentPath(), code)

		// Has been code renamed?
		newPath := code.Path()
		if renameFrom != newPath {
			g.renamed = append(g.renamed, renamedPath{ObjectState: parent, OldPath: oldPath, RenameFrom: renameFrom, NewPath: newPath})
		}
	}

	// Rename code file
	g.updateCodeFilePath(parent, code, oldPathCodeFile)
}

func (g *PathsGenerator) updateCodeFilePath(parent *model.ConfigState, code *model.Code, oldPath string) {
	renameFrom := g.Naming().CodeFilePath(code)
	code.CodeFileName = g.Naming().CodeFileName(code.ComponentId)
	newPath := g.Naming().CodeFilePath(code)
	if renameFrom != newPath {
		g.renamed = append(g.renamed, renamedPath{ObjectState: parent, OldPath: oldPath, RenameFrom: renameFrom, NewPath: newPath})
	}
}
