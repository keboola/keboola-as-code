package transformation

import (
	"context"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// MapAfterLocalLoad - load code blocks from filesystem to Blocks field.
func (m *transformationMapper) MapAfterLocalLoad(ctx context.Context, recipe *model.LocalLoadRecipe) error {
	// Only for transformation config
	if ok, err := m.isTransformationConfig(recipe.Object); err != nil {
		return err
	} else if !ok {
		return nil
	}

	// Create local loader
	l := &localLoader{
		State:           m.state,
		LocalLoadRecipe: recipe,
		logger:          m.logger,
		config:          recipe.Object.(*model.Config),
		blocksDir:       m.state.NamingGenerator().BlocksDir(recipe.ObjectManifest.Path()),
		errors:          errors.NewMultiError(),
	}

	// Load
	return l.loadBlocks()
}

type localLoader struct {
	*state.State
	*model.LocalLoadRecipe
	logger    log.Logger
	config    *model.Config
	blocksDir string
	blocks    []*model.Block
	errors    errors.MultiError
}

func (l *localLoader) loadBlocks() error {
	// Load blocks and codes from filesystem
	for blockIndex, blockDir := range l.blockDirs() {
		block := l.addBlock(blockIndex, blockDir)
		for codeIndex, codeDir := range l.codeDirs(block) {
			l.addCode(block, codeIndex, codeDir)
		}
	}

	// Validate, if all loaded without error
	l.validate()

	l.config.Transformation = &model.Transformation{Blocks: l.blocks}
	return l.errors.ErrorOrNil()
}

func (l *localLoader) validate() {
	if l.errors.Len() == 0 {
		for _, block := range l.blocks {
			if err := l.State.ValidateValue(block); err != nil {
				l.errors.AppendWithPrefixf(err, `block "%s" is not valid`, block.Path())
			}
		}
	}
}

func (l *localLoader) addBlock(blockIndex int, path string) *model.Block {
	block := &model.Block{
		BlockKey: model.BlockKey{
			BranchID:    l.config.BranchID,
			ComponentID: l.config.ComponentID,
			ConfigID:    l.config.ID,
			Index:       blockIndex,
		},
		AbsPath: model.NewAbsPath(
			l.blocksDir,
			path,
		),
		Codes: make([]*model.Code, 0),
	}

	l.ObjectManifest.AddRelatedPath(block.Path())
	l.loadBlockMetaFile(block)
	l.blocks = append(l.blocks, block)

	return block
}

func (l *localLoader) addCode(block *model.Block, codeIndex int, path string) *model.Code {
	code := &model.Code{
		CodeKey: model.CodeKey{
			BranchID:    l.config.BranchID,
			ComponentID: l.config.ComponentID,
			ConfigID:    l.config.ID,
			BlockIndex:  block.Index,
			Index:       codeIndex,
		},
		AbsPath: model.NewAbsPath(
			block.Path(),
			path,
		),
		Scripts: make(model.Scripts, 0),
	}

	l.ObjectManifest.AddRelatedPath(code.Path())
	l.loadCodeMetaFile(code)
	l.addScripts(code)
	block.Codes = append(block.Codes, code)

	return code
}

func (l *localLoader) addScripts(code *model.Code) {
	code.CodeFileName = l.codeFileName(code)
	if code.CodeFileName == "" {
		return
	}

	// Load file content
	file, err := l.Files.
		Load(l.NamingGenerator().CodeFilePath(code)).
		AddMetadata(filesystem.ObjectKeyMetadata, code.Key()).
		SetDescription("code file").
		AddTag(model.FileKindNativeCode).
		ReadFile()
	if err != nil {
		l.errors.Append(err)
		return
	}

	// Split to scripts
	code.Scripts = model.ScriptsFromStr(file.Content, l.config.ComponentID)
	l.logger.Debugf(`Parsed "%d" scripts from "%s"`, len(code.Scripts), file.Path())
}

func (l *localLoader) loadBlockMetaFile(block *model.Block) {
	_, _, err := l.Files.
		Load(l.NamingGenerator().MetaFilePath(block.Path())).
		AddMetadata(filesystem.ObjectKeyMetadata, block.Key()).
		SetDescription("block metadata").
		AddTag(model.FileTypeJSON).
		AddTag(model.FileKindBlockMeta).
		ReadJSONFieldsTo(block, model.MetaFileFieldsTag)
	if err != nil {
		l.errors.Append(err)
	}
}

func (l *localLoader) loadCodeMetaFile(code *model.Code) {
	_, _, err := l.Files.
		Load(l.NamingGenerator().MetaFilePath(code.Path())).
		AddMetadata(filesystem.ObjectKeyMetadata, code.Key()).
		SetDescription("code metadata").
		AddTag(model.FileTypeJSON).
		AddTag(model.FileKindCodeMeta).
		ReadJSONFieldsTo(code, model.MetaFileFieldsTag)
	if err != nil {
		l.errors.Append(err)
	}
}

func (l *localLoader) blockDirs() []string {
	// Check if blocks dir exists
	if !l.ObjectsRoot().IsDir(l.blocksDir) {
		l.errors.Append(errors.Errorf(`missing blocks dir "%s"`, l.blocksDir))
		return nil
	}

	// Track blocks dir
	l.ObjectManifest.AddRelatedPath(l.blocksDir)

	// Track .gitkeep, .gitignore
	if path := filesystem.Join(l.blocksDir, `.gitkeep`); l.ObjectsRoot().IsFile(path) {
		l.ObjectManifest.AddRelatedPath(path)
	}
	if path := filesystem.Join(l.blocksDir, `.gitignore`); l.ObjectsRoot().IsFile(path) {
		l.ObjectManifest.AddRelatedPath(path)
	}

	// Load all dir entries
	dirs, err := l.FileLoader().ReadSubDirs(l.ObjectsRoot(), l.blocksDir)
	if err != nil {
		l.errors.Append(errors.Errorf(`cannot read transformation blocks from "%s": %w`, l.blocksDir, err))
		return nil
	}
	return dirs
}

func (l *localLoader) codeDirs(block *model.Block) []string {
	dirs, err := l.FileLoader().ReadSubDirs(l.ObjectsRoot(), block.Path())
	if err != nil {
		l.errors.Append(errors.Errorf(`cannot read transformation codes from "%s": %w`, block.Path(), err))
		return nil
	}
	return dirs
}

func (l *localLoader) codeFileName(code *model.Code) string {
	// Search for code file, glob "code.*"
	// File can use an old naming, so the file extension is not specified
	matches, err := l.ObjectsRoot().Glob(filesystem.Join(code.Path(), naming.CodeFileName+`.*`))
	if err != nil {
		l.errors.Append(errors.Errorf(`cannot search for code file in %s": %w`, code.Path(), err))
		return ""
	}
	files := make([]string, 0)
	for _, match := range matches {
		relPath, err := filesystem.Rel(code.Path(), match)
		if err != nil {
			l.errors.Append(err)
			continue
		}

		if l.ObjectsRoot().IsFile(match) {
			files = append(files, relPath)
		}
	}

	// No file?
	if len(files) == 0 {
		l.errors.Append(errors.Errorf(`missing code file in "%s"`, code.Path()))
		return ""
	}

	// Multiple files?
	if len(files) > 1 {
		l.errors.Append(errors.Errorf(
			`expected one, but found multiple code files "%s" in "%s"`,
			strings.Join(files, `", "`),
			code.Path()))
		return ""
	}

	// Found
	return files[0]
}
