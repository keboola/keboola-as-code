package transformation

import (
	"fmt"
	"strings"

	"github.com/iancoleman/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/sql"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

type loader struct {
	model.MapperContext
	*model.LocalLoadRecipe
	config    *model.Config
	blocksDir string
	blocks    []*model.Block
	errors    *utils.Error
}

// AfterLocalLoad - load code blocks from filesystem to target config.
func (m *transformationMapper) AfterLocalLoad(recipe *model.LocalLoadRecipe) error {
	// Only for transformation config
	if ok, err := m.isTransformationConfig(recipe.Object); err != nil {
		return err
	} else if !ok {
		return nil
	}

	// Create loader
	l := &loader{
		MapperContext:   m.MapperContext,
		LocalLoadRecipe: recipe,
		config:          recipe.Object.(*model.Config),
		blocksDir:       m.Naming.BlocksDir(recipe.Record.Path()),
		errors:          utils.NewMultiError(),
	}

	// Load
	return l.loadBlocks()
}

func (l *loader) loadBlocks() error {
	// Load blocks and codes from filesystem
	for blockIndex, blockDir := range l.blockDirs() {
		block := l.addBlock(blockIndex, blockDir)
		for codeIndex, codeDir := range l.codeDirs(block) {
			l.addCode(block, codeIndex, codeDir)
		}
	}

	// Validate, if all loaded without error
	l.validate()

	// Set blocks to "parameters.blocks" in the config
	var parameters orderedmap.OrderedMap
	if parametersRaw, found := l.config.Content.Get(`parameters`); found {
		// Use existing map
		parameters = parametersRaw.(orderedmap.OrderedMap)
	} else {
		// Create new map if not exists
		parameters = *utils.NewOrderedMap()
	}

	// Convert []struct to []map
	blocksMap := make([]interface{}, 0)
	for _, block := range l.blocks {
		blockMap := utils.NewOrderedMap()
		utils.ConvertByJson(block, &blockMap)
		blocksMap = append(blocksMap, blockMap)
	}
	parameters.Set("blocks", blocksMap)
	l.config.Content.Set("parameters", parameters)
	l.config.Blocks = l.blocks
	return l.errors.ErrorOrNil()
}

func (l *loader) validate() {
	if l.errors.Len() == 0 {
		for _, block := range l.blocks {
			if err := validator.Validate(block); err != nil {
				l.errors.Append(utils.PrefixError(fmt.Sprintf(`block "%s" is not valid`, block.Path()), err))
			}
		}
	}
}

func (l *loader) addBlock(blockIndex int, path string) *model.Block {
	block := &model.Block{
		BlockKey: model.BlockKey{
			BranchId:    l.config.BranchId,
			ComponentId: l.config.ComponentId,
			ConfigId:    l.config.Id,
			Index:       blockIndex,
		},
		Paths: model.Paths{
			PathInProject: model.NewPathInProject(
				l.blocksDir,
				path,
			),
		},
		Codes: make([]*model.Code, 0),
	}

	l.Record.AddRelatedPath(block.Path())
	l.loadBlockMetaFile(block)
	l.blocks = append(l.blocks, block)

	return block
}

func (l *loader) addCode(block *model.Block, codeIndex int, path string) *model.Code {
	code := &model.Code{
		CodeKey: model.CodeKey{
			BranchId:    l.config.BranchId,
			ComponentId: l.config.ComponentId,
			ConfigId:    l.config.Id,
			BlockIndex:  block.Index,
			Index:       codeIndex,
		},
		Paths: model.Paths{
			PathInProject: model.NewPathInProject(
				block.Path(),
				path,
			),
		},
		Scripts: make([]string, 0),
	}

	l.Record.AddRelatedPath(code.Path())
	l.loadCodeMetaFile(code)
	l.addScripts(code)
	block.Codes = append(block.Codes, code)

	return code
}

func (l *loader) addScripts(code *model.Code) {
	code.CodeFileName = l.codeFileName(code)
	if code.CodeFileName == "" {
		return
	}

	// Load file content
	codeFilePath := l.Naming.CodeFilePath(code)
	file, err := l.Fs.ReadFile(codeFilePath, "code file")
	if err != nil {
		l.errors.Append(err)
		return
	}

	// Split to scripts
	code.Scripts = l.parseScripts(file.Content)
	l.Record.AddRelatedPath(codeFilePath)
	l.Logger.Debugf(`Parsed "%d" scripts from "%s"`, len(code.Scripts), codeFilePath)
}

func (l *loader) loadBlockMetaFile(block *model.Block) {
	path := l.Naming.MetaFilePath(block.Path())
	desc := "block metadata"
	if file, err := l.Fs.ReadJsonFieldsTo(path, desc, block, model.MetaFileTag); err != nil {
		l.errors.Append(err)
	} else if file != nil {
		l.Record.AddRelatedPath(path)
	}
}

func (l *loader) loadCodeMetaFile(code *model.Code) {
	path := l.Naming.MetaFilePath(code.Path())
	desc := "code metadata"
	if file, err := l.Fs.ReadJsonFieldsTo(path, desc, code, model.MetaFileTag); err != nil {
		l.errors.Append(err)
	} else if file != nil {
		l.Record.AddRelatedPath(path)
	}
}

func (l *loader) blockDirs() []string {
	// Check if blocks dir exists
	if !l.Fs.IsDir(l.blocksDir) {
		l.errors.Append(fmt.Errorf(`missing blocks dir "%s"`, l.blocksDir))
		return nil
	}

	// Load all dir entries
	items, err := l.Fs.ReadDir(l.blocksDir)
	if err != nil {
		l.errors.Append(fmt.Errorf(`cannot read transformation blocks from "%s": %w`, l.blocksDir, err))
		return nil
	}

	// Only dirs
	dirs := make([]string, 0)
	for _, item := range items {
		if item.IsDir() {
			dirs = append(dirs, item.Name())
		}
	}
	return dirs
}

func (l *loader) codeDirs(block *model.Block) []string {
	// Load all dir entries
	items, err := l.Fs.ReadDir(block.Path())
	if err != nil {
		l.errors.Append(fmt.Errorf(`cannot read transformation codes from "%s": %w`, block.Path(), err))
		return nil
	}

	// Only dirs
	dirs := make([]string, 0)
	for _, item := range items {
		if item.IsDir() {
			dirs = append(dirs, item.Name())
		}
	}
	return dirs
}

func (l *loader) codeFileName(code *model.Code) string {
	// Search for code file, glob "code.*"
	// File can use an old naming, so the file extension is not specified
	matches, err := l.Fs.Glob(filesystem.Join(code.Path(), model.CodeFileName+`.*`))
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

		if l.Fs.IsFile(match) {
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

func (l *loader) parseScripts(content string) []string {
	switch l.config.ComponentId {
	case `keboola.snowflake-transformation`:
		fallthrough
	case `keboola.synapse-transformation`:
		fallthrough
	case `keboola.oracle-transformation`:
		return sql.Split(content)
	default:
		return []string{strings.TrimSuffix(content, "\n")}
	}
}
