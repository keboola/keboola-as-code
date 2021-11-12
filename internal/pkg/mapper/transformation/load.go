package transformation

import (
	"fmt"
	"strings"

	"github.com/iancoleman/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/strhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

// MapAfterRemoteLoad - load code blocks from API to blocks field.
func (m *transformationMapper) MapAfterRemoteLoad(recipe *model.RemoteLoadRecipe) error {
	// Only for transformation config
	if ok, err := m.isTransformationConfig(recipe.InternalObject); err != nil {
		return err
	} else if !ok {
		return nil
	}
	config := recipe.InternalObject.(*model.Config)

	// Get parameters
	var parameters orderedmap.OrderedMap
	parametersRaw := utils.GetFromMap(config.Content, []string{`parameters`})
	if v, ok := parametersRaw.(orderedmap.OrderedMap); ok {
		parameters = v
	} else {
		parameters = *utils.NewOrderedMap()
	}

	// Get blocks
	var blocks []interface{}
	blocksRaw := utils.GetFromMap(&parameters, []string{`blocks`})
	if v, ok := blocksRaw.([]interface{}); ok {
		blocks = v
	}

	// Remove blocks from config.json
	parameters.Delete(`blocks`)
	config.Content.Set(`parameters`, parameters)

	// Convert map to Block structs
	if err := utils.ConvertByJson(blocks, &config.Blocks); err != nil {
		return err
	}

	// Fill in keys
	for blockIndex, block := range config.Blocks {
		block.BranchId = config.BranchId
		block.ComponentId = config.ComponentId
		block.ConfigId = config.Id
		block.Index = blockIndex
		for codeIndex, code := range block.Codes {
			code.BranchId = config.BranchId
			code.ComponentId = config.ComponentId
			code.ConfigId = config.Id
			code.BlockIndex = block.Index
			code.Index = codeIndex
			for i, script := range code.Scripts {
				code.Scripts[i] = strhelper.NormalizeScript(script)
			}
		}
	}

	// Set paths, if are present
	if recipe.Manifest.Path() != "" {
		blocksDir := m.Naming.BlocksDir(recipe.Manifest.Path())
		for _, block := range config.Blocks {
			if path, found := m.Naming.GetCurrentPath(block.Key()); found {
				block.PathInProject = path
			} else {
				block.PathInProject = m.Naming.BlockPath(blocksDir, block)
			}
			for _, code := range block.Codes {
				if path, found := m.Naming.GetCurrentPath(code.Key()); found {
					code.PathInProject = path
				} else {
					code.PathInProject = m.Naming.CodePath(block.Path(), code)
				}
				code.CodeFileName = m.Naming.CodeFileName(config.ComponentId)
			}
		}
	}

	return nil
}

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
		MapperContext:   m.MapperContext,
		LocalLoadRecipe: recipe,
		config:          recipe.Object.(*model.Config),
		blocksDir:       m.Naming.BlocksDir(recipe.Record.Path()),
		errors:          utils.NewMultiError(),
	}

	// Load
	return l.loadBlocks()
}

type localLoader struct {
	model.MapperContext
	*model.LocalLoadRecipe
	config    *model.Config
	blocksDir string
	blocks    []*model.Block
	errors    *utils.Error
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

	l.config.Blocks = l.blocks
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

	l.Record.AddRelatedPath(block.Path())
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
		Scripts: make([]string, 0),
	}

	l.Record.AddRelatedPath(code.Path())
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
	codeFilePath := l.Naming.CodeFilePath(code)
	file, err := l.Fs.ReadFile(codeFilePath, "code file")
	if err != nil {
		l.errors.Append(err)
		return
	}

	// Split to scripts
	code.Scripts = strhelper.ParseTransformationScripts(file.Content, l.config.ComponentId)
	l.Record.AddRelatedPath(codeFilePath)
	l.Logger.Debugf(`Parsed "%d" scripts from "%s"`, len(code.Scripts), codeFilePath)
}

func (l *localLoader) loadBlockMetaFile(block *model.Block) {
	path := l.Naming.MetaFilePath(block.Path())
	desc := "block metadata"
	if file, err := l.Fs.ReadJsonFieldsTo(path, desc, block, model.MetaFileTag); err != nil {
		l.errors.Append(err)
	} else if file != nil {
		l.Record.AddRelatedPath(path)
	}
}

func (l *localLoader) loadCodeMetaFile(code *model.Code) {
	path := l.Naming.MetaFilePath(code.Path())
	desc := "code metadata"
	if file, err := l.Fs.ReadJsonFieldsTo(path, desc, code, model.MetaFileTag); err != nil {
		l.errors.Append(err)
	} else if file != nil {
		l.Record.AddRelatedPath(path)
	}
}

func (l *localLoader) blockDirs() []string {
	// Check if blocks dir exists
	if !l.Fs.IsDir(l.blocksDir) {
		l.errors.Append(fmt.Errorf(`missing blocks dir "%s"`, l.blocksDir))
		return nil
	}

	// Track blocks dir
	l.Record.AddRelatedPath(l.blocksDir)

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

func (l *localLoader) codeDirs(block *model.Block) []string {
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

func (l *localLoader) codeFileName(code *model.Code) string {
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
