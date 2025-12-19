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
}

// StorageMapping represents an input or output table mapping.
type StorageMapping struct {
	Source      string `json:"source"`
	Destination string `json:"destination"`
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

	// Extract config ID from path (last directory name)
	transformation.ConfigID = filesystem.Base(configPath)

	return transformation, nil
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
