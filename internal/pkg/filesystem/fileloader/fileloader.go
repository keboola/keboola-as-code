package fileloader

import (
	"fmt"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

type loader struct {
	fs filesystem.Fs
}

func New(fs filesystem.Fs) filesystem.FileLoader {
	return &loader{fs: fs}
}

func (l *loader) ReadFile(path, desc string) (*filesystem.File, error) {
	return l.fs.ReadFile(path, desc)
}

// ReadJsonFile to ordered map.
func (l *loader) ReadJsonFile(path, desc string) (*filesystem.JsonFile, error) {
	file, err := l.fs.ReadFile(path, desc)
	if err != nil {
		return nil, err
	}

	jsonFile, err := file.ToJsonFile()
	if err != nil {
		return nil, err
	}

	return jsonFile, nil
}

// ReadJsonFileTo to target struct.
func (l *loader) ReadJsonFileTo(path, desc string, target interface{}) (*filesystem.File, error) {
	file, err := l.fs.ReadFile(path, desc)
	if err != nil {
		return nil, err
	}

	if err := json.DecodeString(file.Content, target); err != nil {
		fileDesc := strings.TrimSpace(file.Desc + " file")
		return nil, utils.PrefixError(fmt.Sprintf("%s \"%s\" is invalid", fileDesc, file.Path), err)
	}

	return file, nil
}

// ReadJsonFieldsTo target struct by tag.
func (l *loader) ReadJsonFieldsTo(path, desc string, target interface{}, tag string) (*filesystem.JsonFile, bool, error) {
	if fields := utils.GetFieldsWithTag(tag, target); len(fields) > 0 {
		if file, err := l.ReadJsonFile(path, desc); err == nil {
			utils.SetFields(fields, file.Content, target)
			return file, true, nil
		} else {
			return nil, false, err
		}
	}

	return nil, false, nil
}

// ReadJsonMapTo tagged field in target struct as ordered map.
func (l *loader) ReadJsonMapTo(path, desc string, target interface{}, tag string) (*filesystem.JsonFile, bool, error) {
	if field := utils.GetOneFieldWithTag(tag, target); field != nil {
		if file, err := l.ReadJsonFile(path, desc); err == nil {
			utils.SetField(field, file.Content, target)
			return file, true, nil
		} else {
			// Set empty map if error occurred
			utils.SetField(field, orderedmap.New(), target)
			return nil, false, err
		}
	}
	return nil, false, nil
}

// ReadFileContentTo to tagged field in target struct as string.
func (l *loader) ReadFileContentTo(path, desc string, target interface{}, tag string) (*filesystem.File, bool, error) {
	if field := utils.GetOneFieldWithTag(tag, target); field != nil {
		if file, err := l.fs.ReadFile(path, desc); err == nil {
			content := strings.TrimRight(file.Content, " \r\n\t")
			utils.SetField(field, content, target)
			return file, true, nil
		} else {
			return nil, false, err
		}
	}
	return nil, false, nil
}
