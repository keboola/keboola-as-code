package main

import (
	"os"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/clifs"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd"
	interactivePrompt "github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt/interactive"
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
	rootCmd := cmd.NewRootCommand(os.Stdin, os.Stdout, os.Stderr, prompt, osEnvs, clifs.New)
	os.Exit(rootCmd.Execute())
}
