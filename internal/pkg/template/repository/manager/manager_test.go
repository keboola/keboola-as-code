// nolint: forbidigo
package manager_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository/manager"
)

func TestNew(t *testing.T) {
	t.Parallel()
	if runtime.GOOS == "windows" {
		t.Skip("unstable on windows - random timeouts")
	}

	// Copy the git repository to temp
	tmpDir := t.TempDir()
	assert.NoError(t, aferofs.CopyFs2Fs(nil, filepath.Join("test", "repository"), nil, tmpDir))
	assert.NoError(t, os.Rename(filepath.Join(tmpDir, ".gittest"), filepath.Join(tmpDir, ".git")))
	ref := model.TemplateRepository{
		Type: model.RepositoryTypeGit,
		Name: repository.DefaultTemplateRepositoryName,
		URL:  fmt.Sprintf("file://%s", tmpDir),
		Ref:  "main",
	}

	// Create manager
	ctx := context.Background()
	d := dependencies.NewMockedDeps(t)
	m, err := manager.New(ctx, d, nil)
	assert.NoError(t, err)

	repo, unlockFn, err := m.Repository(ctx, ref)
	assert.NoError(t, err)
	defer unlockFn()

	assert.True(t, repo.Fs().Exists("example-file.txt"))
}

func TestRepository(t *testing.T) {
	t.Parallel()
	if runtime.GOOS == "windows" {
		t.Skip("unstable on windows - random timeouts")
	}

	// Copy the git repository to temp
	tmpDir := t.TempDir()
	assert.NoError(t, aferofs.CopyFs2Fs(nil, filepath.Join("test", "repository"), nil, tmpDir))
	assert.NoError(t, os.Rename(filepath.Join(tmpDir, ".gittest"), filepath.Join(tmpDir, ".git")))
	repo := model.TemplateRepository{
		Type: model.RepositoryTypeGit,
		Name: repository.DefaultTemplateRepositoryName,
		URL:  fmt.Sprintf("file://%s", tmpDir),
		Ref:  "main",
	}

	// Create manager
	ctx := context.Background()
	d := dependencies.NewMockedDeps(t)
	m, err := manager.New(ctx, d, nil)
	assert.NoError(t, err)

	v, unlockFn1, err := m.Repository(ctx, repo)
	assert.NotNil(t, v)
	assert.NoError(t, err)
	defer unlockFn1()

	v, unlockFn2, err := m.Repository(ctx, repo)
	assert.NotNil(t, v)
	assert.NoError(t, err)
	defer unlockFn2()
}

func TestRepositoryUpdate(t *testing.T) {
	t.Parallel()
	if runtime.GOOS == "windows" {
		t.Skip("unstable on windows - random timeouts")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Copy the git repository to temp
	tmpDir := t.TempDir()
	assert.NoError(t, aferofs.CopyFs2Fs(nil, filepath.Join("test", "repository"), nil, tmpDir))
	assert.NoError(t, os.Rename(filepath.Join(tmpDir, ".gittest"), filepath.Join(tmpDir, ".git")))
	repo := model.TemplateRepository{
		Type: model.RepositoryTypeGit,
		Name: repository.DefaultTemplateRepositoryName,
		URL:  fmt.Sprintf("file://%s", tmpDir),
		Ref:  "main",
	}

	// Create manager
	d := dependencies.NewMockedDeps(t)
	m, err := manager.New(ctx, d, []model.TemplateRepository{repo})
	assert.NoError(t, err)

	// 1. update - no change
	assert.NoError(t, <-m.Update(ctx))

	// Modify git repository
	runGitCommand(t, tmpDir, "reset", "--hard", "HEAD~1")

	// 2. update - change
	assert.NoError(t, <-m.Update(ctx))

	// Check metrics
	histBounds := []float64{0, 5, 10, 25, 50, 75, 100, 250, 500, 750, 1000, 2500, 5000, 7500, 10000} // ms
	d.TestTelemetry().AssertMetrics(t,
		[]metricdata.Metrics{
			{
				Name:        "keboola.go.templates.repo.sync.duration",
				Description: "Templates repository sync duration.",
				Unit:        "ms",
				Data: metricdata.Histogram[float64]{
					Temporality: 1,
					DataPoints: []metricdata.HistogramDataPoint[float64]{
						// Init
						{
							Count:  1,
							Bounds: histBounds,
							Attributes: attribute.NewSet(
								attribute.String("repo.name", "keboola"),
								attribute.String("repo.url", "file://<tmp_dir>"),
								attribute.String("repo.ref", "main"),
								attribute.Bool("is_init", true),
								attribute.Bool("is_success", true),
								attribute.Bool("is_changed", true),
							),
						},
						// Update - no change
						{
							Count:  1,
							Bounds: histBounds,
							Attributes: attribute.NewSet(
								attribute.String("repo.name", "keboola"),
								attribute.String("repo.url", "file://<tmp_dir>"),
								attribute.String("repo.ref", "main"),
								attribute.Bool("is_init", false),
								attribute.Bool("is_success", true),
								attribute.Bool("is_changed", false),
							),
						},
						// Update - changed
						{
							Count:  1,
							Bounds: histBounds,
							Attributes: attribute.NewSet(
								attribute.String("repo.name", "keboola"),
								attribute.String("repo.url", "file://<tmp_dir>"),
								attribute.String("repo.ref", "main"),
								attribute.Bool("is_init", false),
								attribute.Bool("is_success", true),
								attribute.Bool("is_changed", true),
							),
						},
					},
				},
			},
		},
		telemetry.WithMeterAttributeMapper(func(attr attribute.KeyValue) attribute.KeyValue {
			if attr.Key == "repo.url" && strings.HasPrefix(attr.Value.AsString(), "file://") {
				return attribute.String(string(attr.Key), "file://<tmp_dir>")
			}
			return attr
		}),
		telemetry.WithDataPointSortKey(func(attrs attribute.Set) string {
			// Priority: 1. init=true; 2. changed=false
			var out strings.Builder
			if init, _ := attrs.Value("is_init"); init.AsBool() {
				out.WriteByte('0')
			} else {
				out.WriteByte('1')
			}
			if changed, _ := attrs.Value("is_changed"); changed.AsBool() {
				out.WriteByte('1')
			} else {
				out.WriteByte('0')
			}
			return out.String()
		}),
	)
}

func TestDefaultRepositories(t *testing.T) {
	t.Parallel()
	if runtime.GOOS == "windows" {
		t.Skip("unstable on windows - random timeouts")
	}

	// Copy the git repository to temp
	tmpDir := t.TempDir()
	assert.NoError(t, aferofs.CopyFs2Fs(nil, filepath.Join("test", "repository"), nil, tmpDir))
	assert.NoError(t, os.Rename(filepath.Join(tmpDir, ".gittest"), filepath.Join(tmpDir, ".git")))

	// Define default repositories
	gitURL := fmt.Sprintf("file://%s", tmpDir)
	commitHash := "92d0b5f200129303e31feaf201fa0f46b2739782"
	defaultRepositories := []model.TemplateRepository{
		{
			Type: model.RepositoryTypeGit,
			Name: "git repo",
			URL:  gitURL,
			Ref:  "main",
		},
		{
			Type: model.RepositoryTypeDir,
			Name: "dir repo",
			URL:  tmpDir,
		},
	}

	// Create manager
	ctx := context.Background()
	d := dependencies.NewMockedDeps(t)
	m, err := manager.New(ctx, d, defaultRepositories)
	assert.NoError(t, err)

	// Get list of default repositories
	assert.Equal(t, defaultRepositories, m.DefaultRepositories())
	assert.Equal(t, []string{
		fmt.Sprintf("dir:%s", tmpDir),
		fmt.Sprintf("%s:main:%s", gitURL, commitHash),
	}, m.ManagedRepositories())
}

func runGitCommand(t *testing.T, dir string, args ...string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	assert.NoError(t, cmd.Run(), "STDOUT:\n"+stdout.String()+"\n\nSTDERR:\n"+stderr.String())
}
