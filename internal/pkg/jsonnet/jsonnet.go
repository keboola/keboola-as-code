package jsonnet

import (
	"github.com/google/go-jsonnet"
	"github.com/google/go-jsonnet/formatter"
)

func Decode(input string) (string, error) {
	return jsonnet.MakeVM().EvaluateAnonymousSnippet(``, input)
}

func Format(input string) (string, error) {
	return formatter.Format(``, input, DefaultOptions())
}

func DefaultOptions() formatter.Options {
	return formatter.Options{
		Indent:           2,
		MaxBlankLines:    2,
		PrettyFieldNames: true,
		PadArrays:        true,
		PadObjects:       true,
		SortImports:      true,
		StringStyle:      formatter.StringStyleDouble,
		CommentStyle:     formatter.CommentStyleSlash,
	}
}
