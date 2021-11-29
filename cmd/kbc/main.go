package main

import (
	"os"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/cmd"
	interactivePrompt "github.com/keboola/keboola-as-code/internal/pkg/cli/prompt/interactive"
	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
)

func main() {
	// Interactive prompt
	prompt := interactivePrompt.New(os.Stdin, os.Stdout, os.Stderr)

	// Load Os Envs
	osEnvs, err := env.FromOs()
	if err != nil {
		panic(err)
	}

	// Run command
	rootCmd := cmd.NewRootCommand(os.Stdin, os.Stdout, os.Stderr, prompt, osEnvs, aferofs.NewLocalFsFindProjectDir)
	os.Exit(rootCmd.Execute())
}
