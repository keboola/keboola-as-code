// Package data provides functionality for downloading table data for local execution of transformations.
package data

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// DefaultRowLimit is the default number of rows to download per table.
const DefaultRowLimit = 1000

type dependencies interface {
	KeboolaProjectAPI() *keboola.AuthorizedAPI
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
}

// Options for the local data download operation.
type Options struct {
	// ConfigPath is the path to the config directory (e.g., "main/transformations/keboola.python-transformation-v2/my-transform").
	ConfigPath string
	// RowLimit is the maximum number of rows to download per table. 0 means unlimited.
	RowLimit uint
}

// Run downloads table data for local execution.
func Run(ctx context.Context, projectState *project.State, o Options, d dependencies) (err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.project.local.data")
	defer span.End(&err)

	logger := d.Logger()

	// Find the config by path
	configState, err := findConfigByPath(projectState, o.ConfigPath)
	if err != nil {
		return err
	}

	config := configState.Local

	logger.Infof(ctx, `Downloading data for config "%s" (%s)`, config.Name, config.ComponentID)

	// Determine the data directory path using the config's path from the manifest
	configDir := configState.Path()
	dataDir := filepath.Join(projectState.ObjectsRoot().BasePath(), configDir, naming.DataDir)

	// Always create the data directory structure
	if err := createDataDirs(dataDir); err != nil {
		return errors.PrefixError(err, "failed to create data directories")
	}

	// Always generate config.json (Common Interface format)
	if err := generateConfigJSON(config, dataDir); err != nil {
		return errors.PrefixError(err, "failed to generate config.json")
	}

	// Get input mappings from config content
	inputTables, inputFiles, err := getInputMappings(config)
	if err != nil {
		return errors.PrefixError(err, "failed to get input mappings")
	}

	if len(inputTables) == 0 && len(inputFiles) == 0 {
		logger.Info(ctx, "No input mappings found in config.")
		logger.Infof(ctx, `Data directory created at "%s"`, dataDir)
		return nil
	}

	// Download input tables
	errs := errors.NewMultiError()
	for _, table := range inputTables {
		if err := downloadTable(ctx, d, table, dataDir, o.RowLimit); err != nil {
			errs.AppendWithPrefixf(err, `failed to download table "%s"`, table.Source)
		}
	}

	// Download input files
	for _, file := range inputFiles {
		if err := downloadFile(ctx, d, file, dataDir); err != nil {
			errs.AppendWithPrefixf(err, `failed to download file "%s"`, file.Source)
		}
	}

	if errs.Len() > 0 {
		return errs
	}

	logger.Infof(ctx, `Data downloaded to "%s"`, dataDir)
	return nil
}

// findConfigByPath finds a config by its relative path.
func findConfigByPath(projectState *project.State, configPath string) (*model.ConfigState, error) {
	// Normalize path
	configPath = strings.TrimPrefix(configPath, "./")
	configPath = strings.TrimSuffix(configPath, "/")

	// Search through all objects
	for _, objectState := range projectState.All() {
		configState, ok := objectState.(*model.ConfigState)
		if !ok {
			continue
		}

		if !configState.HasLocalState() {
			continue
		}

		// Check if path matches
		objectPath := configState.Path()
		if objectPath == configPath || strings.HasSuffix(objectPath, "/"+configPath) {
			return configState, nil
		}
	}

	return nil, errors.Errorf(`config not found at path "%s"`, configPath)
}

// InputTable represents an input table mapping.
type InputTable struct {
	Source        string
	Destination   string
	Columns       []string
	WhereColumn   string
	WhereOperator string
	WhereValues   []string
	ChangedSince  string
	Limit         int
}

// InputFile represents an input file mapping.
type InputFile struct {
	Tags        []string
	Source      string
	Destination string
	Query       string
}

// getInputMappings extracts input table and file mappings from config content.
func getInputMappings(config *model.Config) ([]InputTable, []InputFile, error) {
	if config.Content == nil {
		return nil, nil, nil
	}

	// Get storage section
	storageRaw, ok := config.Content.Get("storage")
	if !ok {
		return nil, nil, nil
	}

	storage, ok := storageRaw.(*orderedmap.OrderedMap)
	if !ok {
		if storageMap, ok := storageRaw.(map[string]any); ok {
			storage = orderedmap.FromPairs(mapToPairs(storageMap))
		} else {
			return nil, nil, nil
		}
	}

	// Get input section
	inputRaw, ok := storage.Get("input")
	if !ok {
		return nil, nil, nil
	}

	input, ok := inputRaw.(*orderedmap.OrderedMap)
	if !ok {
		if inputMap, ok := inputRaw.(map[string]any); ok {
			input = orderedmap.FromPairs(mapToPairs(inputMap))
		} else {
			return nil, nil, nil
		}
	}

	var tables []InputTable
	var files []InputFile

	// Parse tables
	if tablesRaw, ok := input.Get("tables"); ok {
		tables = parseInputTables(tablesRaw)
	}

	// Parse files
	if filesRaw, ok := input.Get("files"); ok {
		files = parseInputFiles(filesRaw)
	}

	return tables, files, nil
}

// parseInputTables parses the input tables from config content.
func parseInputTables(tablesRaw any) []InputTable {
	var result []InputTable

	tables, ok := tablesRaw.([]any)
	if !ok {
		return result
	}

	for _, tableRaw := range tables {
		tableMap, ok := tableRaw.(map[string]any)
		if !ok {
			if tableOm, ok := tableRaw.(*orderedmap.OrderedMap); ok {
				tableMap = orderedMapToPlainMap(tableOm)
			} else {
				continue
			}
		}

		table := InputTable{
			Source:      getString(tableMap, "source"),
			Destination: getString(tableMap, "destination"),
		}

		if cols, ok := tableMap["columns"].([]any); ok {
			for _, col := range cols {
				if colStr, ok := col.(string); ok {
					table.Columns = append(table.Columns, colStr)
				}
			}
		}

		table.WhereColumn = getString(tableMap, "where_column")
		table.WhereOperator = getString(tableMap, "where_operator")
		if vals, ok := tableMap["where_values"].([]any); ok {
			for _, val := range vals {
				if valStr, ok := val.(string); ok {
					table.WhereValues = append(table.WhereValues, valStr)
				}
			}
		}

		table.ChangedSince = getString(tableMap, "changed_since")
		if limit, ok := tableMap["limit"].(float64); ok {
			table.Limit = int(limit)
		}

		if table.Source != "" && table.Destination != "" {
			result = append(result, table)
		}
	}

	return result
}

// parseInputFiles parses the input files from config content.
func parseInputFiles(filesRaw any) []InputFile {
	var result []InputFile

	files, ok := filesRaw.([]any)
	if !ok {
		return result
	}

	for _, fileRaw := range files {
		fileMap, ok := fileRaw.(map[string]any)
		if !ok {
			if fileOm, ok := fileRaw.(*orderedmap.OrderedMap); ok {
				fileMap = orderedMapToPlainMap(fileOm)
			} else {
				continue
			}
		}

		file := InputFile{
			Source:      getString(fileMap, "source"),
			Destination: getString(fileMap, "destination"),
			Query:       getString(fileMap, "query"),
		}

		if tags, ok := fileMap["tags"].([]any); ok {
			for _, tag := range tags {
				if tagStr, ok := tag.(string); ok {
					file.Tags = append(file.Tags, tagStr)
				}
			}
		}

		result = append(result, file)
	}

	return result
}

// createDataDirs creates the data directory structure for local execution.
func createDataDirs(dataDir string) error {
	dirs := []string{
		filepath.Join(dataDir, "in", "tables"),
		filepath.Join(dataDir, "in", "files"),
		filepath.Join(dataDir, "out", "tables"),
		filepath.Join(dataDir, "out", "files"),
	}

	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0o755); err != nil { //nolint:forbidigo
			return err
		}
	}

	return nil
}

// downloadTable downloads a table from Storage API.
func downloadTable(ctx context.Context, d dependencies, table InputTable, dataDir string, globalLimit uint) error {
	logger := d.Logger()
	api := d.KeboolaProjectAPI()

	// Parse table ID to get bucket and table name
	parts := strings.Split(table.Source, ".")
	if len(parts) < 3 {
		return errors.Errorf("invalid table ID format: %s", table.Source)
	}

	tableKey := keboola.TableKey{
		BranchID: keboola.BranchID(0), // Use default branch
		TableID:  keboola.MustParseTableID(table.Source),
	}

	// Determine row limit
	limit := globalLimit
	if table.Limit > 0 && (limit == 0 || uint(table.Limit) < limit) {
		limit = uint(table.Limit)
	}

	logger.Infof(ctx, `Downloading table "%s" (limit: %d rows)`, table.Source, limit)

	// Build preview options
	opts := []keboola.PreviewOption{}
	if limit > 0 {
		opts = append(opts, keboola.WithLimitRows(limit))
	}
	if len(table.Columns) > 0 {
		opts = append(opts, keboola.WithExportColumns(table.Columns...))
	}
	if table.ChangedSince != "" {
		opts = append(opts, keboola.WithChangedSince(table.ChangedSince))
	}
	if table.WhereColumn != "" && len(table.WhereValues) > 0 {
		op := parseCompareOp(table.WhereOperator)
		opts = append(opts, keboola.WithWhere(table.WhereColumn, op, table.WhereValues))
	}

	// Preview the table
	result, err := api.PreviewTableRequest(tableKey, opts...).Send(ctx)
	if err != nil {
		return errors.Errorf("failed to preview table: %w", err)
	}

	// Write CSV file
	destPath := filepath.Join(dataDir, "in", "tables", table.Destination)
	if err := writeTableCSV(destPath, result); err != nil {
		return errors.Errorf("failed to write CSV: %w", err)
	}

	// Write manifest file
	manifestPath := destPath + ".manifest"
	if err := writeTableManifest(manifestPath, table, result); err != nil {
		return errors.Errorf("failed to write manifest: %w", err)
	}

	logger.Infof(ctx, `  -> Downloaded %d rows to "%s"`, len(result.Rows), destPath)
	return nil
}

// writeTableCSV writes table data to a CSV file.
func writeTableCSV(path string, table *keboola.TablePreview) error {
	file, err := os.Create(path) //nolint:forbidigo
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header
	if err := writer.Write(table.Columns); err != nil {
		return err
	}

	// Write rows (table.Rows is [][]string)
	for _, row := range table.Rows {
		if err := writer.Write(row); err != nil {
			return err
		}
	}

	return nil
}

// writeTableManifest writes a table manifest file in Common Interface format.
func writeTableManifest(path string, table InputTable, preview *keboola.TablePreview) error {
	manifest := map[string]any{
		"id":          table.Source,
		"destination": table.Destination,
		"columns":     preview.Columns,
	}

	if table.WhereColumn != "" {
		manifest["where_column"] = table.WhereColumn
		manifest["where_operator"] = table.WhereOperator
		manifest["where_values"] = table.WhereValues
	}

	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(path, data, 0o644) //nolint:forbidigo
}

// downloadFile downloads a file from Storage API.
func downloadFile(ctx context.Context, d dependencies, file InputFile, dataDir string) error {
	logger := d.Logger()
	api := d.KeboolaProjectAPI()

	// If source is a file ID, download directly
	if file.Source != "" {
		logger.Infof(ctx, `Downloading file "%s"`, file.Source)

		// List files to find the one matching source
		files, err := api.ListFilesRequest(keboola.BranchID(0)).Send(ctx)
		if err != nil {
			return errors.Errorf("failed to list files: %w", err)
		}

		// Find the file by name or ID
		var targetFile *keboola.File
		for _, f := range *files {
			if f.Name == file.Source || fmt.Sprintf("%d", f.FileID) == file.Source {
				targetFile = f
				break
			}
		}

		if targetFile == nil {
			return errors.Errorf("file not found: %s", file.Source)
		}

		// Download the file
		destPath := filepath.Join(dataDir, "in", "files", file.Destination)
		if file.Destination == "" {
			destPath = filepath.Join(dataDir, "in", "files", targetFile.Name)
		}

		// Get download credentials
		creds, err := api.GetFileWithCredentialsRequest(targetFile.FileKey).Send(ctx)
		if err != nil {
			return errors.Errorf("failed to get file credentials: %w", err)
		}

		// Download using the SDK
		reader, err := keboola.DownloadReader(ctx, creds)
		if err != nil {
			return errors.Errorf("failed to create download reader: %w", err)
		}
		defer reader.Close()

		outFile, err := os.Create(destPath) //nolint:forbidigo
		if err != nil {
			return err
		}
		defer outFile.Close()

		if _, err := outFile.ReadFrom(reader); err != nil {
			return errors.Errorf("failed to download file content: %w", err)
		}

		logger.Infof(ctx, `  -> Downloaded to "%s"`, destPath)
	}

	return nil
}

// generateConfigJSON generates config.json for applications in Common Interface format.
func generateConfigJSON(config *model.Config, dataDir string) error {
	// Get parameters from config content
	configData := map[string]any{}

	if config.Content != nil {
		if params, ok := config.Content.Get("parameters"); ok {
			if paramsMap, ok := params.(*orderedmap.OrderedMap); ok {
				configData["parameters"] = orderedMapToPlainMap(paramsMap)
			} else if paramsPlain, ok := params.(map[string]any); ok {
				configData["parameters"] = paramsPlain
			}
		}
	}

	// Write config.json
	data, err := json.MarshalIndent(configData, "", "  ")
	if err != nil {
		return err
	}

	configPath := filepath.Join(dataDir, "config.json")
	return os.WriteFile(configPath, data, 0o644) //nolint:forbidigo
}

// parseCompareOp parses a comparison operator string.
func parseCompareOp(op string) keboola.CompareOp {
	switch op {
	case "eq", "=", "==":
		return keboola.CompareEq
	case "ne", "!=":
		return keboola.CompareNe
	case "gt", ">":
		return keboola.CompareGt
	case "ge", ">=":
		return keboola.CompareGe
	case "lt", "<":
		return keboola.CompareLt
	case "le", "<=":
		return keboola.CompareLe
	default:
		return keboola.CompareEq
	}
}

// Helper functions

func getString(m map[string]any, key string) string {
	if val, ok := m[key]; ok {
		if str, ok := val.(string); ok {
			return str
		}
	}
	return ""
}

func mapToPairs(m map[string]any) []orderedmap.Pair {
	pairs := make([]orderedmap.Pair, 0, len(m))
	for k, v := range m {
		pairs = append(pairs, orderedmap.Pair{Key: k, Value: v})
	}
	return pairs
}

func orderedMapToPlainMap(om *orderedmap.OrderedMap) map[string]any {
	if om == nil {
		return nil
	}
	result := make(map[string]any)
	for _, key := range om.Keys() {
		val, _ := om.Get(key)
		if nested, ok := val.(*orderedmap.OrderedMap); ok {
			result[key] = orderedMapToPlainMap(nested)
		} else {
			result[key] = val
		}
	}
	return result
}
