package search

import (
	"fmt"
	"strings"

	"github.com/spf13/cast"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

// Branches searches for branches by ID and name.
func Branches(all []*model.Branch, str string) []*model.Branch {
	matches := make([]*model.Branch, 0)
	for _, object := range all {
		if matchObjectIdOrName(str, object) {
			matches = append(matches, object)
		}
	}
	return matches
}

// Branch searches for branch by ID and name.
func Branch(all []*model.Branch, str string) (*model.Branch, error) {
	branches := Branches(all, str)
	switch len(branches) {
	case 1:
		// ok, one match
		return branches[0], nil
	case 0:
		return nil, fmt.Errorf(`no branch matches the specified "%s"`, str)
	default:
		return nil, fmt.Errorf(`multiple branches match the specified "%s"`, str)
	}
}

// Configs searches for configs by ID and name.
func Configs(all []*model.ConfigWithRows, str string) []*model.ConfigWithRows {
	matches := make([]*model.ConfigWithRows, 0)
	for _, object := range all {
		if matchObjectIdOrName(str, object) {
			matches = append(matches, object)
		}
	}
	return matches
}

// ConfigsForTemplateInstance searches for configs created by template instance.
func ConfigsForTemplateInstance(all []*model.ConfigWithRows, instanceId string) []*model.ConfigWithRows {
	matches := make([]*model.ConfigWithRows, 0)
	for _, object := range all {
		if object.Metadata.InstanceId() == instanceId {
			matches = append(matches, object)
		}
	}
	return matches
}

// Config searches for config by ID and name.
func Config(all []*model.ConfigWithRows, str string) (*model.ConfigWithRows, error) {
	configs := Configs(all, str)
	switch len(configs) {
	case 1:
		// ok, one match
		return configs[0], nil
	case 0:
		return nil, fmt.Errorf(`no config matches the specified "%s"`, str)
	default:
		return nil, fmt.Errorf(`multiple configs match the specified "%s"`, str)
	}
}

// ConfigRows searches for config row by ID and name.
func ConfigRows(all []*model.ConfigRow, str string) []*model.ConfigRow {
	matches := make([]*model.ConfigRow, 0)
	for _, object := range all {
		if matchObjectIdOrName(str, object) {
			matches = append(matches, object)
		}
	}
	return matches
}

// ConfigRow searches for config row by ID and name.
func ConfigRow(all []*model.ConfigRow, str string) (*model.ConfigRow, error) {
	rows := ConfigRows(all, str)
	switch len(rows) {
	case 1:
		// ok, one match
		return rows[0], nil
	case 0:
		return nil, fmt.Errorf(`no row matches the specified "%s"`, str)
	default:
		return nil, fmt.Errorf(`multiple rows match the specified "%s"`, str)
	}
}

type objectIdAndName interface {
	ObjectId() string
	ObjectName() string
}

// matchObjectIdOrName returns true if str == objectId or objectName contains str.
func matchObjectIdOrName(str string, object objectIdAndName) bool {
	if cast.ToString(object.ObjectId()) == str {
		return true
	}

	// Matched by name
	return strings.Contains(strings.ToLower(object.ObjectName()), strings.ToLower(str))
}
