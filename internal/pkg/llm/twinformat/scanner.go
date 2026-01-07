package twinformat

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// ScannedTransformation represents a transformation scanned from local files.
type ScannedTransformation struct {
	ComponentID  string
	ConfigID     string
	Name         string
	Description  string
	IsDisabled   bool
	Path         string
	InputTables  []StorageMapping
	OutputTables []StorageMapping
	Blocks       []*ScannedBlock // Code blocks from local files
}

// ScannedBlock represents a code block scanned from local files.
type ScannedBlock struct {
	Name  string
	Codes []*ScannedCode
}

// ScannedCode represents a code script within a block.
type ScannedCode struct {
	Name     string
	Language string // sql, python, r
	Script   string
}

// ScannerDependencies defines dependencies for the Scanner.
type ScannerDependencies interface {
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
	Fs() filesystem.Fs
}

// Scanner scans local transformation files.
type Scanner struct {
	logger    log.Logger
	telemetry telemetry.Telemetry
	fs        filesystem.Fs
}

// NewScanner creates a new Scanner instance.
func NewScanner(d ScannerDependencies) *Scanner {
	return &Scanner{
		logger:    d.Logger(),
		telemetry: d.Telemetry(),
		fs:        d.Fs(),
	}
}

// ScanTransformations scans the main/transformation/ directory for transformations.
func (s *Scanner) ScanTransformations(ctx context.Context, projectDir string) (transformations []*ScannedTransformation, err error) {
	ctx, span := s.telemetry.Tracer().Start(ctx, "keboola.go.twinformat.scanner.ScanTransformations")
	defer span.End(&err)

	transformationDir := filesystem.Join(projectDir, "main", "transformation")

	// Check if transformation directory exists
	if !s.fs.Exists(ctx, transformationDir) {
		s.logger.Info(ctx, "No transformation directory found")
		return []*ScannedTransformation{}, nil
	}

	// List component directories (e.g., keboola.snowflake-transformation)
	componentDirs, err := s.fs.ReadDir(ctx, transformationDir)
	if err != nil {
		return nil, errors.Errorf("failed to read transformation directory: %w", err)
	}

	transformations = make([]*ScannedTransformation, 0)

	for _, componentDir := range componentDirs {
		if !componentDir.IsDir() {
			continue
		}

		componentID := componentDir.Name()
		componentPath := filesystem.Join(transformationDir, componentID)

		// List config directories within the component
		configDirs, err := s.fs.ReadDir(ctx, componentPath)
		if err != nil {
			s.logger.Warnf(ctx, "Failed to read component directory %s: %v", componentID, err)
			continue
		}

		for _, configDir := range configDirs {
			if !configDir.IsDir() {
				continue
			}

			configPath := filesystem.Join(componentPath, configDir.Name())
			transformation, err := s.scanTransformation(ctx, componentID, configPath)
			if err != nil {
				s.logger.Warnf(ctx, "Failed to scan transformation at %s: %v", configPath, err)
				continue
			}

			if transformation != nil {
				transformations = append(transformations, transformation)
			}
		}
	}

	s.logger.Infof(ctx, "Scanned %d transformations from local files", len(transformations))
	return transformations, nil
}

// scanTransformation scans a single transformation directory.
func (s *Scanner) scanTransformation(ctx context.Context, componentID, configPath string) (*ScannedTransformation, error) {
	transformation := &ScannedTransformation{
		ComponentID:  componentID,
		Path:         configPath,
		InputTables:  make([]StorageMapping, 0),
		OutputTables: make([]StorageMapping, 0),
	}

	// Read config.json
	configFile := filesystem.Join(configPath, "config.json")
	if s.fs.Exists(ctx, configFile) {
		if err := s.readConfigJSON(ctx, configFile, transformation); err != nil {
			return nil, errors.Errorf("failed to read config.json: %w", err)
		}
	}

	// Read meta.json
	metaFile := filesystem.Join(configPath, "meta.json")
	if s.fs.Exists(ctx, metaFile) {
		if err := s.readMetaJSON(ctx, metaFile, transformation); err != nil {
			return nil, errors.Errorf("failed to read meta.json: %w", err)
		}
	}

	// Read description.md
	descFile := filesystem.Join(configPath, "description.md")
	if s.fs.Exists(ctx, descFile) {
		content, err := s.fs.ReadFile(ctx, filesystem.NewFileDef(descFile))
		if err == nil {
			transformation.Description = strings.TrimSpace(content.Content)
		}
	}

	// Scan code blocks
	blocksDir := filesystem.Join(configPath, "blocks")
	if s.fs.Exists(ctx, blocksDir) {
		blocks, err := s.scanCodeBlocks(ctx, blocksDir)
		if err != nil {
			s.logger.Warnf(ctx, "Failed to scan code blocks at %s: %v", blocksDir, err)
		} else {
			transformation.Blocks = blocks
		}
	}

	// Extract config ID from path (last directory name)
	transformation.ConfigID = filesystem.Base(configPath)

	return transformation, nil
}

// scanCodeBlocks scans the blocks/ subdirectory for code blocks.
func (s *Scanner) scanCodeBlocks(ctx context.Context, blocksDir string) ([]*ScannedBlock, error) {
	blockDirs, err := s.fs.ReadDir(ctx, blocksDir)
	if err != nil {
		return nil, errors.Errorf("failed to read blocks directory: %w", err)
	}

	blocks := make([]*ScannedBlock, 0)

	for _, blockDir := range blockDirs {
		if !blockDir.IsDir() {
			continue
		}

		blockPath := filesystem.Join(blocksDir, blockDir.Name())
		block := &ScannedBlock{
			Name:  blockDir.Name(),
			Codes: make([]*ScannedCode, 0),
		}

		// Read block name from meta.json if it exists
		blockMetaFile := filesystem.Join(blockPath, "meta.json")
		if s.fs.Exists(ctx, blockMetaFile) {
			if name, err := s.readBlockName(ctx, blockMetaFile); err == nil && name != "" {
				block.Name = name
			}
		}

		// Scan codes within the block
		codeDirs, err := s.fs.ReadDir(ctx, blockPath)
		if err != nil {
			s.logger.Warnf(ctx, "Failed to read block directory %s: %v", blockPath, err)
			continue
		}

		for _, codeDir := range codeDirs {
			if !codeDir.IsDir() {
				continue
			}

			codePath := filesystem.Join(blockPath, codeDir.Name())
			code := s.scanCode(ctx, codePath, codeDir.Name())
			if code != nil {
				block.Codes = append(block.Codes, code)
			}
		}

		if len(block.Codes) > 0 {
			blocks = append(blocks, block)
		}
	}

	return blocks, nil
}

// scanCode scans a single code directory for the script file.
func (s *Scanner) scanCode(ctx context.Context, codePath, defaultName string) *ScannedCode {
	code := &ScannedCode{
		Name: defaultName,
	}

	// Read code name from meta.json if it exists
	codeMetaFile := filesystem.Join(codePath, "meta.json")
	if s.fs.Exists(ctx, codeMetaFile) {
		if name, err := s.readBlockName(ctx, codeMetaFile); err == nil && name != "" {
			code.Name = name
		}
	}

	// Look for code file (code.sql, code.py, code.r)
	codeFiles := []struct {
		file     string
		language string
	}{
		{"code.sql", "sql"},
		{"code.py", "python"},
		{"code.r", "r"},
	}

	for _, cf := range codeFiles {
		codeFile := filesystem.Join(codePath, cf.file)
		if s.fs.Exists(ctx, codeFile) {
			content, err := s.fs.ReadFile(ctx, filesystem.NewFileDef(codeFile))
			if err != nil {
				continue
			}
			code.Script = content.Content
			code.Language = cf.language
			return code
		}
	}

	// No code file found
	return nil
}

// readBlockName reads the name from a meta.json file.
func (s *Scanner) readBlockName(ctx context.Context, path string) (string, error) {
	content, err := s.fs.ReadFile(ctx, filesystem.NewFileDef(path))
	if err != nil {
		return "", err
	}

	var meta struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal([]byte(content.Content), &meta); err != nil {
		return "", err
	}

	return meta.Name, nil
}

// configJSON represents the structure of config.json.
type configJSON struct {
	Storage *storageConfig `json:"storage"`
}

// storageConfig represents the storage section of config.json.
type storageConfig struct {
	Input  *storageIO `json:"input"`
	Output *storageIO `json:"output"`
}

// storageIO represents input or output storage configuration.
type storageIO struct {
	Tables []StorageMapping `json:"tables"`
}

// readConfigJSON reads and parses config.json.
func (s *Scanner) readConfigJSON(ctx context.Context, path string, t *ScannedTransformation) error {
	content, err := s.fs.ReadFile(ctx, filesystem.NewFileDef(path))
	if err != nil {
		return err
	}

	var config configJSON
	if err := json.Unmarshal([]byte(content.Content), &config); err != nil {
		return errors.Errorf("failed to parse config.json: %w", err)
	}

	if config.Storage != nil {
		if config.Storage.Input != nil && config.Storage.Input.Tables != nil {
			t.InputTables = config.Storage.Input.Tables
		}
		if config.Storage.Output != nil && config.Storage.Output.Tables != nil {
			t.OutputTables = config.Storage.Output.Tables
		}
	}

	return nil
}

// metaJSON represents the structure of meta.json.
type metaJSON struct {
	Name       string `json:"name"`
	IsDisabled bool   `json:"isDisabled"`
}

// readMetaJSON reads and parses meta.json.
func (s *Scanner) readMetaJSON(ctx context.Context, path string, t *ScannedTransformation) error {
	content, err := s.fs.ReadFile(ctx, filesystem.NewFileDef(path))
	if err != nil {
		return err
	}

	var meta metaJSON
	if err := json.Unmarshal([]byte(content.Content), &meta); err != nil {
		return errors.Errorf("failed to parse meta.json: %w", err)
	}

	t.Name = meta.Name
	t.IsDisabled = meta.IsDisabled

	return nil
}
