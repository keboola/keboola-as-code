package transformation

import (
	"fmt"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

// MapAfterLocalLoad - load code blocks from filesystem to Blocks field.
func (m *transformationMapper) MapAfterLocalLoad(recipe *model.LocalLoadRecipe) error {
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
		errors:          utils.NewMultiError(),
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
	errors    *utils.MultiError
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
			if err := validator.Validate(block); err != nil {
				l.errors.Append(utils.PrefixError(fmt.Sprintf(`block "%s" is not valid`, block.Path()), err))
			}
		}
	}
}

func (l *localLoader) addBlock(blockIndex int, path string) *model.Block {
	block := &model.Block{
		BlockKey: model.BlockKey{
			BranchId:    l.config.BranchId,
			ComponentId: l.config.ComponentId,
			ConfigId:    l.config.Id,
			Index:       blockIndex,
		},
		PathInProject: model.NewPathInProject(
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
			BranchId:    l.config.BranchId,
			ComponentId: l.config.ComponentId,
			ConfigId:    l.config.Id,
			BlockIndex:  block.Index,
			Index:       codeIndex,
		},
		PathInProject: model.NewPathInProject(
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
	codeFilePath := l.NamingGenerator().CodeFilePath(code)
	file, err := l.Fs().ReadFile(codeFilePath, "code file")
	if err != nil {
		l.errors.Append(err)
		return
	}
	l.Files.
		Add(file).
		AddTag(model.FileKindNativeCode)

	// Split to scripts
	code.Scripts = model.ScriptsFromStr(file.Content, l.config.ComponentId)
	l.logger.Debugf(`Parsed "%d" scripts from "%s"`, len(code.Scripts), codeFilePath)
}

func (l *localLoader) loadBlockMetaFile(block *model.Block) {
	path := l.NamingGenerator().MetaFilePath(block.Path())
	desc := "block metadata"
	if file, err := l.Fs().ReadJsonFieldsTo(path, desc, block, model.MetaFileFieldsTag); err != nil {
		l.errors.Append(err)
	} else if file != nil {
		l.Files.
			Add(file).
			AddTag(model.FileTypeJson).
			AddTag(model.FileKindBlockMeta)
	}
}

func (l *localLoader) loadCodeMetaFile(code *model.Code) {
	path := l.NamingGenerator().MetaFilePath(code.Path())
	desc := "code metadata"
	if file, err := l.Fs().ReadJsonFieldsTo(path, desc, code, model.MetaFileFieldsTag); err != nil {
		l.errors.Append(err)
	} else if file != nil {
		l.Files.
			Add(file).
			AddTag(model.FileTypeJson).
			AddTag(model.FileKindCodeMeta)
	}
}

func (l *localLoader) blockDirs() []string {
	// Check if blocks dir exists
	if !l.Fs().IsDir(l.blocksDir) {
		l.errors.Append(fmt.Errorf(`missing blocks dir "%s"`, l.blocksDir))
		return nil
	}

	// Track blocks dir
	l.ObjectManifest.AddRelatedPath(l.blocksDir)

	// Load all dir entries
	dirs, err := filesystem.ReadSubDirs(l.Fs(), l.blocksDir)
	if err != nil {
		l.errors.Append(fmt.Errorf(`cannot read transformation blocks from "%s": %w`, l.blocksDir, err))
		return nil
	}
	return dirs
}

func (l *localLoader) codeDirs(block *model.Block) []string {
	dirs, err := filesystem.ReadSubDirs(l.Fs(), block.Path())
	if err != nil {
		l.errors.Append(fmt.Errorf(`cannot read transformation codes from "%s": %w`, block.Path(), err))
		return nil
	}
	return dirs
}

func (l *localLoader) codeFileName(code *model.Code) string {
	// Search for code file, glob "code.*"
	// File can use an old naming, so the file extension is not specified
	matches, err := l.Fs().Glob(filesystem.Join(code.Path(), naming.CodeFileName+`.*`))
	if err != nil {
		l.errors.Append(fmt.Errorf(`cannot search for code file in %s": %w`, code.Path(), err))
		return ""
	}
	files := make([]string, 0)
	for _, match := range matches {
		relPath, err := filesystem.Rel(code.Path(), match)
		if err != nil {
			l.errors.Append(err)
			continue
		}

		if l.Fs().IsFile(match) {
			files = append(files, relPath)
		}
	}

	// No file?
	if len(files) == 0 {
		l.errors.Append(fmt.Errorf(`missing code file in "%s"`, code.Path()))
		return ""
	}

	// Multiple files?
	if len(files) > 1 {
		l.errors.Append(fmt.Errorf(
			`expected one, but found multiple code files "%s" in "%s"`,
			strings.Join(files, `", "`),
			code.Path(),
		))
		return ""
	}

	// Found
	return files[0]
}
