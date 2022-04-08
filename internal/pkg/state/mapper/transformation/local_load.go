package transformation

import (
	"fmt"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local/relatedpaths"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

type localLoadContext struct {
	*model.LocalLoadRecipe
	state          *local.State
	logger         log.Logger
	transformation *model.Config
	basePath       model.AbsPath
	blocks         []*model.Block
	relatedPaths   *relatedpaths.Paths
}

// MapAfterLocalLoad - load code blocks from filesystem to Blocks field.
func (m *transformationLocalMapper) MapAfterLocalLoad(recipe *model.LocalLoadRecipe) error {
	// Only for transformation config
	if ok, err := m.isTransformation(recipe.Object.Key()); err != nil {
		return err
	} else if !ok {
		return nil
	}
	transformation := recipe.Object.(*model.Config)

	basePath, err := m.state.GetPath(transformation)
	if err != nil {
		return err
	}

	relatedPaths, err := m.state.GetRelatedPaths(transformation)
	if err != nil {
		return err
	}

	l := &localLoadContext{
		LocalLoadRecipe: recipe,
		state:           m.state,
		logger:          m.logger,
		transformation:  transformation,
		basePath:        basePath,
		relatedPaths:    relatedPaths,
	}

	// Load
	return l.load()
}

func (c *localLoadContext) load() error {
	// Search for dirs with blocks
	blocksDirs, err := c.blockDirs()
	if err != nil {
		return err
	}

	// Load blocks and codes from filesystem
	errs := errors.NewMultiError()
	for blockIndex, blockDir := range blocksDirs {
		if err := c.loadBlock(blockIndex, blockDir); err != nil {
			errs.AppendWithPrefix(fmt.Sprintf(`invalid block "%s"`, blockDir.Base()), err)
		}
	}

	c.transformation.Transformation = &model.Transformation{Blocks: c.blocks}
	return errs.ErrorOrNil()
}

func (c *localLoadContext) loadBlock(blockIndex int, blockDir model.AbsPath) error {
	// Create struct
	block := &model.Block{
		BlockKey: model.BlockKey{
			Parent: c.transformation.ConfigKey,
			Index:  blockIndex,
		},
		Codes: make([]*model.Code, 0),
	}

	// Add block dir to the related paths
	c.relatedPaths.Add(blockDir.String())

	// Attach block dir to the naming
	if err := c.state.NamingRegistry().Attach(block.BlockKey, blockDir); err != nil {
		return err
	}

	// Load meta file
	errs := errors.NewMultiError()
	if err := c.loadBlockMetaFile(block, blockDir); err != nil {
		errs.Append(err)
	}

	// Search for dirs with codes
	codesDirs, err := c.codeDirs(block, blockDir.String())
	if err != nil {
		errs.Append(err)
	}

	// Load codes
	for codeIndex, codeDir := range codesDirs {
		if err := c.loadCode(block, codeIndex, codeDir); err != nil {
			errs.AppendWithPrefix(fmt.Sprintf(`invalid code "%s"`, codeDir.Base()), err)
		}
	}

	// Validate block with codes
	if errors.Len() == 0 {
		if err := validator.Validate(c.state.Ctx(), block); err != nil {
			errs.Append(err)
		}
	}

	if errors.Len() == 0 {
		c.blocks = append(c.blocks, block)
	}

	return errs.ErrorOrNil()
}

func (c *localLoadContext) loadCode(block *model.Block, codeIndex int, codeDir model.AbsPath) error {
	code := &model.Code{
		CodeKey: model.CodeKey{
			Parent: block.BlockKey,
			Index:  codeIndex,
		},
		Scripts: make(model.Scripts, 0),
	}

	// Add code dir to the related paths
	c.relatedPaths.Add(codeDir.String())

	// Attach code dir to the naming
	if err := c.state.NamingRegistry().Attach(code.CodeKey, codeDir); err != nil {
		return err
	}

	// Load meta file
	errs := errors.NewMultiError()
	if err := c.loadCodeMetaFile(code, codeDir); err != nil {
		errs.Append(err)
	}

	// Load code file
	if err := c.addScripts(code, codeDir); err != nil {
		errs.Append(err)
	}

	if errors.Len() == 0 {
		block.Codes = append(block.Codes, code)
	}

	return errs.ErrorOrNil()
}

func (c *localLoadContext) addScripts(code *model.Code, codeDir model.AbsPath) error {
	// Find code file
	fileName, err := c.codeFileName(codeDir)
	if err != nil {
		return err
	} else if fileName == "" {
		return nil
	}

	// Load file content
	file, err := c.Files.
		Load(filesystem.Join(codeDir.String(), fileName)).
		SetDescription("code file").
		AddTag(model.FileKindNativeCode).
		ReadFile()
	if err != nil {
		return err
	}

	// Split to scripts
	code.Scripts = model.ScriptsFromStr(file.Content, c.transformation.ComponentId)
	c.logger.Debugf(`Parsed "%d" scripts from "%s"`, len(code.Scripts), file.Path())
	return nil
}

func (c *localLoadContext) loadBlockMetaFile(block *model.Block, blockDir model.AbsPath) error {
	_, _, err := c.Files.
		Load(c.state.NamingGenerator().MetaFilePath(blockDir)).
		SetDescription("block metadata").
		AddTag(model.FileTypeJson).
		AddTag(model.FileKindBlockMeta).
		ReadJsonFieldsTo(block, model.MetaFileFieldsTag)
	return err
}

func (c *localLoadContext) loadCodeMetaFile(code *model.Code, codeDir model.AbsPath) error {
	_, _, err := c.Files.
		Load(c.state.NamingGenerator().MetaFilePath(codeDir)).
		SetDescription("code metadata").
		AddTag(model.FileTypeJson).
		AddTag(model.FileKindCodeMeta).
		ReadJsonFieldsTo(code, model.MetaFileFieldsTag)
	return err
}

func (c *localLoadContext) codeFileName(codeDir model.AbsPath) (string, error) {
	fs := c.state.ObjectsRoot()

	// Search for code file, glob "code.*"
	// File can use an old naming, so the file extension is not specified
	matches, err := fs.Glob(filesystem.Join(codeDir.String(), naming.CodeFileName+`.*`))
	if err != nil {
		return "", fmt.Errorf(`cannot search for code file in %s": %w`, codeDir.String(), err)
	}

	errs := errors.NewMultiError()
	files := make([]string, 0)
	for _, match := range matches {
		relPath, err := filesystem.Rel(codeDir.String(), match)
		if err != nil {
			errs.Append(err)
			continue
		}

		if fs.IsFile(match) {
			files = append(files, relPath)
		}
	}

	if errors.Len() > 0 {
		return "", errors
	}

	// No file?
	if len(files) == 0 {
		return "", fmt.Errorf(`missing code file in "%s"`, codeDir.String())
	}

	// Multiple files?
	if len(files) > 1 {
		return "", fmt.Errorf(
			`expected one, but found multiple code files "%s" in "%s"`,
			strings.Join(files, `", "`),
			codeDir.String(),
		)
	}

	// Found
	return files[0], nil
}

func (c *localLoadContext) blockDirs() ([]model.AbsPath, error) {
	fs := c.state.ObjectsRoot()
	blocksDir := c.state.NamingGenerator().BlocksDir(c.basePath)

	// Check if blocks dir exists
	if !fs.IsDir(blocksDir.String()) {
		return nil, fmt.Errorf(`missing blocks dir "%s"`, blocksDir)
	}

	// Add blocks dir to the related paths
	c.relatedPaths.Add(blocksDir.String())

	// Track .gitkeep, .gitignore
	if path := filesystem.Join(blocksDir.String(), `.gitkeep`); fs.IsFile(path) {
		c.relatedPaths.Add(path)
	}
	if path := filesystem.Join(blocksDir.String(), `.gitignore`); fs.IsFile(path) {
		c.relatedPaths.Add(path)
	}

	// Read all sub-dirs
	blocksDirs, err := filesystem.ReadSubDirs(fs, blocksDir.String())
	if err != nil {
		return nil, fmt.Errorf(`cannot read transformation blocks from "%s": %w`, blocksDir, err)
	}

	// Convert to []AbsPath
	out := make([]model.AbsPath, len(blocksDirs))
	for i, dir := range blocksDirs {
		out[i] = model.NewAbsPath(blocksDir.ParentPath(), filesystem.Join(blocksDir.RelativePath(), dir))
	}
	return out, nil
}

func (c *localLoadContext) codeDirs(block *model.Block, blockDir string) ([]model.AbsPath, error) {
	fs := c.state.ObjectsRoot()

	// Read all sub-dirs
	codesDirs, err := filesystem.ReadSubDirs(fs, blockDir)
	if err != nil {
		return nil, fmt.Errorf(`cannot read transformation codes from "%s": %w`, block.String(), err)
	}

	// Convert to []AbsPath
	out := make([]model.AbsPath, len(codesDirs))
	for i, dir := range codesDirs {
		out[i] = model.NewAbsPath(blockDir, dir)
	}
	return out, nil
}
