package dialog_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/relvacode/iso8601"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/local/create/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/local/create/row"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/remote/create/branch"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	createConfig "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/create/config"
	createRow "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/create/row"
	createBranch "github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/create/branch"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/create/table"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

func TestAskCreateBranch(t *testing.T) {
	t.Parallel()
	dialog, _, console := createDialogs(t, true)

	// Interaction
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		assert.NoError(t, console.ExpectString("Enter a name for the new branch"))

		assert.NoError(t, console.SendLine(`Foo Bar`))

		assert.NoError(t, console.ExpectEOF())
	}()

	// Run
	opts, err := branch.AskCreateBranch(dialog, configmap.NewValue("Foo Bar"))
	assert.NoError(t, err)
	assert.NoError(t, console.Tty().Close())
	wg.Wait()
	assert.NoError(t, console.Close())

	// Assert
	assert.Equal(t, createBranch.Options{
		Name: `Foo Bar`,
		Pull: true,
	}, opts)
}

func TestAskCreateConfig(t *testing.T) {
	t.Parallel()

	// Test dependencies
	dialog, _, console := createDialogs(t, true)
	fs := aferofs.NewMemoryFs()
	d := dependencies.NewMocked(t)
	ctx := context.Background()

	// Create manifest file
	manifestContent := `
{
  "version": 1,
  "project": {"id": %d, "apiHost": "%s"},
  "naming": {
    "branch": "{branch_name}",
    "config": "{component_type}/{component_id}/{config_name}",
    "configRow": "rows/{config_row_name}",
    "sharedCodeConfig": "_shared/{target_component_id}",
    "sharedCodeConfigRow": "codes/{config_row_name}"
  },
  "branches": [{"id": 123, "path": "main"}]
}
`
	assert.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(
		filesystem.Join(filesystem.MetadataDir, manifest.FileName),
		fmt.Sprintf(manifestContent, 123, `foo.bar.com`),
	)))

	// Create branch files
	assert.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(filesystem.Join(`main`, naming.MetaFile), `{"name": "Main"}`)))
	assert.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(filesystem.Join(`main`, naming.DescriptionFile), ``)))

	// Load project
	projectState, err := d.MockedProject(fs).LoadState(loadState.Options{LoadLocalState: true}, d)
	assert.NoError(t, err)

	// Interaction
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		assert.NoError(t, console.ExpectString("Select the target branch"))

		assert.NoError(t, console.SendEnter()) // enter - Main

		assert.NoError(t, console.ExpectString("Select the target component"))

		assert.NoError(t, console.SendLine("extractor generic\n"))

		assert.NoError(t, console.ExpectString("Enter a name for the new config"))

		assert.NoError(t, console.SendLine(`Foo Bar`))

		assert.NoError(t, console.ExpectEOF())
	}()

	// Run
	opts, err := config.AskCreateConfig(projectState, dialog, d, config.Flags{})
	assert.NoError(t, err)
	assert.NoError(t, console.Tty().Close())
	wg.Wait()
	assert.NoError(t, console.Close())

	// Assert
	assert.Equal(t, createConfig.Options{
		BranchID:    123,
		ComponentID: `ex-generic-v2`,
		Name:        `Foo Bar`,
	}, opts)
}

func TestAskCreateRow(t *testing.T) {
	t.Parallel()

	// Test dependencies
	dialog, _, console := createDialogs(t, true)
	fs := aferofs.NewMemoryFs()
	d := dependencies.NewMocked(t)
	ctx := context.Background()

	// Create manifest file
	manifestContent := `
{
  "version": 1,
  "project": {"id": %d, "apiHost": "%s"},
  "naming": {
    "branch": "{branch_name}",
    "config": "{component_type}/{component_id}/{config_name}",
    "configRow": "rows/{config_row_name}",
    "sharedCodeConfig": "_shared/{target_component_id}",
    "sharedCodeConfigRow": "codes/{config_row_name}"
  },
  "branches": [{"id": 123, "path": "main"}],
  "configurations": [
    {
      "branchId": 123,
      "componentId": "keboola.ex-db-mysql",
      "id": "456",
      "path": "extractor/keboola.ex-db-mysql/my-config"
    }
  ]
}
`
	assert.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(
		filesystem.Join(filesystem.MetadataDir, manifest.FileName),
		fmt.Sprintf(manifestContent, 123, `foo.bar.com`),
	)))

	// Create branch files
	assert.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(filesystem.Join(`main`, naming.MetaFile), `{"name": "Main"}`)))
	assert.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(filesystem.Join(`main`, naming.DescriptionFile), ``)))

	// Create config files
	configDir := filesystem.Join(`main`, `extractor`, `keboola.ex-db-mysql`, `my-config`)
	assert.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(filesystem.Join(configDir, naming.MetaFile), `{"name": "My Config"}`)))
	assert.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(filesystem.Join(configDir, naming.ConfigFile), `{}`)))
	assert.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(filesystem.Join(configDir, naming.DescriptionFile), ``)))

	// Test dependencies
	projectState, err := d.MockedProject(fs).LoadState(loadState.Options{LoadLocalState: true}, d)
	assert.NoError(t, err)

	// Interaction
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		assert.NoError(t, console.ExpectString("Select the target branch"))

		assert.NoError(t, console.SendEnter()) // enter - My Config

		assert.NoError(t, console.ExpectString("Select the target config"))

		assert.NoError(t, console.SendEnter()) // enter - My Config

		assert.NoError(t, console.ExpectString("Enter a name for the new config row"))

		assert.NoError(t, console.SendLine(`Foo Bar`))

		assert.NoError(t, console.ExpectEOF())
	}()

	// Run
	opts, err := row.AskCreateRow(projectState, dialog, row.Flags{})
	assert.NoError(t, err)
	assert.NoError(t, console.Tty().Close())
	wg.Wait()
	assert.NoError(t, console.Close())

	// Assert
	assert.Equal(t, createRow.Options{
		BranchID:    123,
		ComponentID: `keboola.ex-db-mysql`,
		ConfigID:    `456`,
		Name:        `Foo Bar`,
	}, opts)
}

func TestAskCreate(t *testing.T) {
	t.Parallel()

	args := []string{}

	var buckets []*keboola.Bucket

	branch, bucket := getBranchAndBucket()

	buckets = append(buckets, bucket)

	t.Run("columns-from interactive", func(t *testing.T) {
		t.Parallel()
		// Test dependencies
		dialog, _, console := createDialogs(t, true)

		// Set fake file editor
		dialog.Prompt.(*interactive.Prompt).SetEditor(`true`)
		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			defer wg.Done()

			assert.NoError(t, console.ExpectString("Select a bucket:"))

			assert.NoError(t, console.Send("in.c-test_1214124"))

			assert.NoError(t, console.SendEnter())

			assert.NoError(t, console.ExpectString("Enter the table name."))

			assert.NoError(t, console.ExpectString("Table name:"))

			assert.NoError(t, console.Send("table1"))

			assert.NoError(t, console.SendEnter())

			assert.NoError(t, console.ExpectString("Want to define column types?"))

			assert.NoError(t, console.Send("Y"))

			assert.NoError(t, console.SendEnter()) // confirm

			assert.NoError(t, console.ExpectString("Columns definitions"))

			assert.NoError(t, console.SendEnter())

			assert.NoError(t, console.ExpectString("Select columns for primary key"))

			assert.NoError(t, console.ExpectString("id"))

			assert.NoError(t, console.ExpectString("name"))

			assert.NoError(t, console.SendSpace())

			assert.NoError(t, console.SendEnter())

			assert.NoError(t, console.ExpectString("Select columns for primary key: id"))
		}()

		res, err := dialog.AskCreateTable(args, branch.BranchKey, buckets)
		assert.NoError(t, err)
		wg.Wait()

		assert.Equal(t, table.Options{
			CreateTableRequest: keboola.CreateTableRequest{
				TableDefinition: keboola.TableDefinition{
					PrimaryKeyNames: []string{"id"},
					Columns: []keboola.Column{
						{
							Name: "id",
							Definition: keboola.ColumnDefinition{
								Type:     "VARCHAR",
								Nullable: false,
							},
							BaseType: "STRING",
						},
						{
							Name: "name",
							Definition: keboola.ColumnDefinition{
								Type:     "VARCHAR",
								Nullable: false,
							},
							BaseType: "STRING",
						},
					},
				},
				Name: "table1",
			},
			BucketKey: keboola.BucketKey{
				BranchID: branch.ID,
				BucketID: bucket.BucketID,
			},
		}, res)
	})

	t.Run("columns name interactive", func(t *testing.T) {
		t.Parallel()
		// Test dependencies
		dialog, _, console := createDialogs(t, true)

		// Set fake file editor
		dialog.Prompt.(*interactive.Prompt).SetEditor(`true`)
		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			defer wg.Done()

			assert.NoError(t, console.ExpectString("Select a bucket:"))

			assert.NoError(t, console.Send("in.c-test_1214124"))

			assert.NoError(t, console.SendEnter())

			assert.NoError(t, console.ExpectString("Enter the table name."))

			assert.NoError(t, console.ExpectString("Table name:"))

			assert.NoError(t, console.Send("table1"))

			assert.NoError(t, console.SendEnter())

			assert.NoError(t, console.ExpectString("Want to define column types?"))

			assert.NoError(t, console.Send("N"))

			assert.NoError(t, console.SendEnter()) // confirm

			assert.NoError(t, console.ExpectString("Enter a comma-separated list of column names."))

			assert.NoError(t, console.Send("id,name"))

			assert.NoError(t, console.SendEnter())

			assert.NoError(t, console.ExpectString("Select columns for primary key"))

			assert.NoError(t, console.ExpectString("id"))

			assert.NoError(t, console.ExpectString("name"))

			assert.NoError(t, console.SendSpace())

			assert.NoError(t, console.SendEnter())

			assert.NoError(t, console.ExpectString("Select columns for primary key: id"))
		}()

		res, err := dialog.AskCreateTable(args, branch.BranchKey, buckets)
		assert.NoError(t, err)
		wg.Wait()

		assert.Equal(t, table.Options{
			CreateTableRequest: keboola.CreateTableRequest{
				TableDefinition: keboola.TableDefinition{
					PrimaryKeyNames: []string{"id"},
					Columns: []keboola.Column{
						{
							Name: "id",
							Definition: keboola.ColumnDefinition{
								Type:     "STRING",
								Nullable: false,
							},
							BaseType: "STRING",
						},
						{
							Name: "name",
							Definition: keboola.ColumnDefinition{
								Type:     "STRING",
								Nullable: false,
							},
							BaseType: "STRING",
						},
					},
				},
				Name: "table1",
			},
			BucketKey: keboola.BucketKey{
				BranchID: branch.ID,
				BucketID: bucket.BucketID,
			},
		}, res)
	})

	t.Run("columns-from flag", func(t *testing.T) {
		t.Parallel()
		// Test dependencies
		dialog, o, console := createDialogs(t, true)

		// Set fake file editor
		dialog.Prompt.(*interactive.Prompt).SetEditor(`true`)
		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			defer wg.Done()

			assert.NoError(t, console.ExpectString("Select a bucket:"))

			assert.NoError(t, console.Send("in.c-test_1214124"))

			assert.NoError(t, console.SendEnter())

			assert.NoError(t, console.ExpectString("Enter the table name."))

			assert.NoError(t, console.ExpectString("Table name:"))

			assert.NoError(t, console.Send("table1"))

			assert.NoError(t, console.SendEnter())

			assert.NoError(t, console.ExpectString("Select columns for primary key"))

			assert.NoError(t, console.ExpectString("id"))

			assert.NoError(t, console.ExpectString("name"))

			assert.NoError(t, console.SendSpace())

			assert.NoError(t, console.SendEnter())

			assert.NoError(t, console.ExpectString("Select columns for primary key: id"))
		}()

		tempDir := t.TempDir()

		// Create a temporary file within the temporary directory
		tempFile, err := os.Create(filepath.Join(tempDir, "foo.json")) // nolint:forbidigo
		require.NoError(t, err)

		defer tempFile.Close()

		// Write content to the temporary file
		_, err = tempFile.Write([]byte(d.ColumnsInput()))
		require.NoError(t, err)

		// Get the file path of the temporary file
		filePath := tempFile.Name()

		// set flag columns-from
		o.Set("columns-from", filePath)
		res, err := dialog.AskCreateTable(args, branch.BranchKey, buckets)
		assert.NoError(t, err)
		wg.Wait()

		assert.Equal(t, table.Options{
			CreateTableRequest: keboola.CreateTableRequest{
				TableDefinition: keboola.TableDefinition{
					PrimaryKeyNames: []string{"id"},
					Columns: []keboola.Column{
						{
							Name: "id",
							Definition: keboola.ColumnDefinition{
								Type:     "INT",
								Nullable: false,
							},
							BaseType: "NUMERIC",
						},
						{
							Name: "name",
							Definition: keboola.ColumnDefinition{
								Type:     "STRING",
								Nullable: false,
							},
							BaseType: "STRING",
						},
					},
				},
				Name: "table1",
			},
			BucketKey: keboola.BucketKey{
				BranchID: branch.ID,
				BucketID: bucket.BucketID,
			},
		}, res)
	})

	t.Run("columns name from flag", func(t *testing.T) {
		t.Parallel()
		// Test dependencies
		dialog, o, console := createDialogs(t, true)

		// Set fake file editor
		dialog.Prompt.(*interactive.Prompt).SetEditor(`true`)
		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			defer wg.Done()

			assert.NoError(t, console.ExpectString("Select a bucket:"))

			assert.NoError(t, console.Send("in.c-test_1214124"))

			assert.NoError(t, console.SendEnter())

			assert.NoError(t, console.ExpectString("Enter the table name."))

			assert.NoError(t, console.ExpectString("Table name:"))

			assert.NoError(t, console.Send("table1"))

			assert.NoError(t, console.SendEnter())

			assert.NoError(t, console.ExpectString("Select columns for primary key"))

			assert.NoError(t, console.ExpectString("id"))

			assert.NoError(t, console.ExpectString("name"))

			assert.NoError(t, console.SendSpace())

			assert.NoError(t, console.SendEnter())

			assert.NoError(t, console.ExpectString("Select columns for primary key: id"))
		}()

		// set flag --columns
		o.Set("columns", "id,name")
		res, err := dialog.AskCreateTable(args, branch.BranchKey, buckets)
		assert.NoError(t, err)
		wg.Wait()

		assert.Equal(t, table.Options{
			CreateTableRequest: keboola.CreateTableRequest{
				TableDefinition: keboola.TableDefinition{
					PrimaryKeyNames: []string{"id"},
					Columns: []keboola.Column{
						{
							Name: "id",
							Definition: keboola.ColumnDefinition{
								Type:     "STRING",
								Nullable: false,
							},
							BaseType: "STRING",
						},
						{
							Name: "name",
							Definition: keboola.ColumnDefinition{
								Type:     "STRING",
								Nullable: false,
							},
							BaseType: "STRING",
						},
					},
				},
				Name: "table1",
			},
			BucketKey: keboola.BucketKey{
				BranchID: branch.ID,
				BucketID: bucket.BucketID,
			},
		}, res)
	})
}

func getBranchAndBucket() (*keboola.Branch, *keboola.Bucket) {
	branch := &keboola.Branch{
		BranchKey: keboola.BranchKey{
			ID: 123,
		},
		Name:        "testBranch",
		Description: "",
		Created:     iso8601.Time{},
		IsDefault:   false,
	}

	bucket := &keboola.Bucket{
		BucketKey: keboola.BucketKey{
			BranchID: branch.ID,
			BucketID: keboola.BucketID{
				Stage:      keboola.BucketStageIn,
				BucketName: fmt.Sprintf("c-test_%d", 1214124),
			},
		},
	}
	return branch, bucket
}
