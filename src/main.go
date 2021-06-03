package main

import (
	"keboola-as-code/src/cli"
)

func main() {
	// Run command
	commander := cli.NewCommander()
	commander.Execute()
}
