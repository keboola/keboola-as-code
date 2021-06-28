package main

import (
	"keboola-as-code/src/cli"
	"keboola-as-code/src/interaction"
	"os"
)

func main() {
	// Run command
	prompt := interaction.NewPrompt(os.Stdin, os.Stdout, os.Stderr)
	cmd := cli.NewRootCommand(os.Stdin, os.Stdout, os.Stderr, prompt)
	cmd.Execute()
}
