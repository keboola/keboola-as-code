package transformation

import (
	"fmt"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

type localLoadContext struct {
	*state.State
	*model.LocalLoadRecipe
	logger    log.Logger
	config    *model.Config
	blocksDir string
	blocks    []*model.Block
	errors    *utils.MultiError
}

// MapAfterLocalLoad - load code blocks from filesystem to Blocks field.
func (m *transformationLocalMapper) MapAfterLocalLoad(recipe *model.LocalLoadRecipe) error {
	// Only for transformation config
	if ok, err := m.isTransformation(recipe.Object.Key()); err != nil {
		return err
	} else if !ok {
		return nil
	}

	// Create local loader
	l := &localLoadContext{
		State:           m.state,
		LocalLoadRecipe: recipe,
		logger:          m.logger,
		config:          recipe.Object.(*model.Config),
		blocksDir:       m.state.NamingGenerator().BlocksDir(recipe.ObjectManifest.String()),
		errors:          utils.NewMultiError(),
	}

	// Load
	return l.loadBlocks()
}

func (c *localLoadContext) loadBlocks() error {
	// Load blocks and codes from filesystem
	for blockIndex, blockDir := range c.blockDirs() {
		block := c.addBlock(blockIndex, blockDir)
		for codeIndex, codeDir := range c.codeDirs(block) {
			c.addCode(block, codeIndex, codeDir)
		}
	}

	// Validate, if all loaded without error
	c.validate()

	c.config.Transformation = &model.Transformation{Blocks: c.blocks}
	return c.errors.ErrorOrNil()
}

func (c *localLoadContext) validate() {
	if c.errors.Len() == 0 {
		for _, block := range c.blocks {
			if err := validator.Validate(c.State.Ctx(), block); err != nil {
				c.errors.Append(utils.PrefixError(fmt.Sprintf(`block "%s" is not valid`, block.String()), err))
			}
		}
	}
}

func (c *localLoadContext) addBlock(blockIndex int, path string) *model.Block {
	block := &model.Block{
		BlockKey: model.BlockKey{
			BranchId:    c.config.BranchId,
			ComponentId: c.config.ComponentId,
			ConfigId:    c.config.Id,
			Index:       blockIndex,
		},
		AbsPath: model.NewAbsPath(
			c.blocksDir,
			path,
		),
		Codes: make([]*model.Code, 0),
	}

	c.ObjectManifest.AddRelatedPath(block.String())
	c.loadBlockMetaFile(block)
	c.blocks = append(c.blocks, block)

	return block
}

func (c *localLoadContext) addCode(block *model.Block, codeIndex int, path string) *model.Code {
	code := &model.Code{
		CodeKey: model.CodeKey{
			BranchId:    c.config.BranchId,
			ComponentId: c.config.ComponentId,
			ConfigId:    c.config.Id,
			BlockIndex:  block.Index,
			Index:       codeIndex,
		},
		AbsPath: model.NewAbsPath(
			block.String(),
			path,
		),
		Scripts: make(model.Scripts, 0),
	}

	c.ObjectManifest.AddRelatedPath(code.String())
	c.loadCodeMetaFile(code)
	c.addScripts(code)
	block.Codes = append(block.Codes, code)

	return code
}

func (c *localLoadContext) addScripts(code *model.Code) {
	code.CodeFileName = c.codeFileName(code)
	if code.CodeFileName == "" {
		return
	}

	// Load file content
	file, err := c.Files.
		Load(c.NamingGenerator().CodeFilePath(code)).
		SetDescription("code file").
		AddTag(model.FileKindNativeCode).
		ReadFile()
	if err != nil {
		c.errors.Append(err)
		return
	}

	// Split to scripts
	code.Scripts = model.ScriptsFromStr(file.Content, c.config.ComponentId)
	c.logger.Debugf(`Parsed "%d" scripts from "%s"`, len(code.Scripts), file.Path())
}

func (c *localLoadContext) loadBlockMetaFile(block *model.Block) {
	_, _, err := c.Files.
		Load(c.NamingGenerator().MetaFilePath(block.String())).
		SetDescription("block metadata").
		AddTag(model.FileTypeJson).
		AddTag(model.FileKindBlockMeta).
		ReadJsonFieldsTo(block, model.MetaFileFieldsTag)
	if err != nil {
		c.errors.Append(err)
	}
}

func (c *localLoadContext) loadCodeMetaFile(code *model.Code) {
	_, _, err := c.Files.
		Load(c.NamingGenerator().MetaFilePath(code.String())).
		SetDescription("code metadata").
		AddTag(model.FileTypeJson).
		AddTag(model.FileKindCodeMeta).
		ReadJsonFieldsTo(code, model.MetaFileFieldsTag)
	if err != nil {
		c.errors.Append(err)
	}
}

func (c *localLoadContext) blockDirs() []string {
	// Check if blocks dir exists
	if !c.ObjectsRoot().IsDir(c.blocksDir) {
		c.errors.Append(fmt.Errorf(`missing blocks dir "%s"`, c.blocksDir))
		return nil
	}

	// Track blocks dir
	c.ObjectManifest.AddRelatedPath(c.blocksDir)

	// Track .gitkeep, .gitignore
	if path := filesystem.Join(c.blocksDir, `.gitkeep`); c.ObjectsRoot().IsFile(path) {
		c.ObjectManifest.AddRelatedPath(path)
	}
	if path := filesystem.Join(c.blocksDir, `.gitignore`); c.ObjectsRoot().IsFile(path) {
		c.ObjectManifest.AddRelatedPath(path)
	}

	// Load all dir entries
	dirs, err := filesystem.ReadSubDirs(c.ObjectsRoot(), c.blocksDir)
	if err != nil {
		c.errors.Append(fmt.Errorf(`cannot read transformation blocks from "%s": %w`, c.blocksDir, err))
		return nil
	}
	return dirs
}

func (c *localLoadContext) codeDirs(block *model.Block) []string {
	dirs, err := filesystem.ReadSubDirs(c.ObjectsRoot(), block.String())
	if err != nil {
		c.errors.Append(fmt.Errorf(`cannot read transformation codes from "%s": %w`, block.String(), err))
		return nil
	}
	return dirs
}

func (c *localLoadContext) codeFileName(code *model.Code) string {
	// Search for code file, glob "code.*"
	// File can use an old naming, so the file extension is not specified
	matches, err := c.ObjectsRoot().Glob(filesystem.Join(code.String(), naming.CodeFileName+`.*`))
	if err != nil {
		c.errors.Append(fmt.Errorf(`cannot search for code file in %s": %w`, code.String(), err))
		return ""
	}
	files := make([]string, 0)
	for _, match := range matches {
		relPath, err := filesystem.Rel(code.String(), match)
		if err != nil {
			c.errors.Append(err)
			continue
		}

		if c.ObjectsRoot().IsFile(match) {
			files = append(files, relPath)
		}
	}

	// No file?
	if len(files) == 0 {
		c.errors.Append(fmt.Errorf(`missing code file in "%s"`, code.String()))
		return ""
	}

	// Multiple files?
	if len(files) > 1 {
		c.errors.Append(fmt.Errorf(
			`expected one, but found multiple code files "%s" in "%s"`,
			strings.Join(files, `", "`),
			code.String(),
		))
		return ""
	}

	// Found
	return files[0]
}
