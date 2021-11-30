package helpmsg

import (
	"embed"
	"strings"
)

// nolint:gochecknoglobals
//go:embed msg/*
var msgs embed.FS

func Read(path string) string {
	if content, err := msgs.ReadFile(`msg/` + path + `.txt`); err == nil {
		return strings.TrimRight(string(content), " \n\r")
	} else {
		panic(err)
	}
}
