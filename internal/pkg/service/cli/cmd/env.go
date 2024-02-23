package cmd

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

func loadEnvFiles(ctx context.Context, logger log.Logger, osEnvs *env.Map, fs filesystem.Fs) *env.Map {
	// File system basePath = projectDir, so here we are using current/top level dir
	projectDir := `.` // nolint
	workingDir := fs.WorkingDir()

	// Dirs with ENVs files
	dirs := make([]string, 0)
	dirs = append(dirs, workingDir)
	if workingDir != projectDir {
		dirs = append(dirs, projectDir)
	}

	// Load ENVs from files
	return env.LoadDotEnv(ctx, logger, osEnvs, fs, dirs)
}
