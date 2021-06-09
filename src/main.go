package main

import (
	"keboola-as-code/src/cli"
	"os"
)

func main() {
	// Run command
	prompt := cli.NewPrompt(os.Stdin, os.Stdout, os.Stderr)
	cmd := cli.NewRootCommand(os.Stdin, os.Stdout, os.Stderr, prompt)
	cmd.Execute()
}
