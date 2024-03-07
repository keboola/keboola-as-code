package search

import (
	"strings"

	"github.com/spf13/cast"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// Branches searches for branches by ID and name.
func Branches(all []*model.Branch, str string) []*model.Branch {
	matches := make([]*model.Branch, 0)
	for _, object := range all {
		if matchObjectIDOrName(str, object) {
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
		return nil, errors.Errorf(`no branch matches the specified "%s"`, str)
	default:
		return nil, errors.Errorf(`multiple branches match the specified "%s"`, str)
	}
}

// Configs searches for configs by ID and name.
func Configs(all []*model.ConfigWithRows, str string) []*model.ConfigWithRows {
	matches := make([]*model.ConfigWithRows, 0)
	for _, object := range all {
		if matchObjectIDOrName(str, object) {
			matches = append(matches, object)
		}
	}
	return matches
}

// ConfigsForTemplateInstance searches for configs created by template instance.
func ConfigsForTemplateInstance(all []*model.ConfigWithRows, instanceID string) []*model.ConfigWithRows {
	matches := make([]*model.ConfigWithRows, 0)
	for _, object := range all {
		if object.Metadata.InstanceID() == instanceID {
			matches = append(matches, object)
		}
	}
	return matches
}

// ConfigsByTemplateInstance group configurations by the instance ID.
func ConfigsByTemplateInstance(all []*model.ConfigWithRows) map[string][]*model.ConfigWithRows {
	out := make(map[string][]*model.ConfigWithRows)
	for _, object := range all {
		if instanceID := object.Metadata.InstanceID(); instanceID != "" {
			out[instanceID] = append(out[instanceID], object)
		}
	}
	return out
}

// Config searches for config by ID and name.
func Config(all []*model.ConfigWithRows, str string) (*model.ConfigWithRows, error) {
	configs := Configs(all, str)
	switch len(configs) {
	case 1:
		// ok, one match
		return configs[0], nil
	case 0:
		return nil, errors.Errorf(`no config matches the specified "%s"`, str)
	default:
		return nil, errors.Errorf(`multiple configs match the specified "%s"`, str)
	}
}

// ConfigRows searches for config row by ID and name.
func ConfigRows(all []*model.ConfigRow, str string) []*model.ConfigRow {
	matches := make([]*model.ConfigRow, 0)
	for _, object := range all {
		if matchObjectIDOrName(str, object) {
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
		return nil, errors.Errorf(`no row matches the specified "%s"`, str)
	default:
		return nil, errors.Errorf(`multiple rows match the specified "%s"`, str)
	}
}

type objectIDAndName interface {
	ObjectID() string
	ObjectName() string
}

// matchObjectIDOrName returns true if str == objectId or objectName contains str.
func matchObjectIDOrName(str string, object objectIDAndName) bool {
	if cast.ToString(object.ObjectID()) == str {
		return true
	}

	// Matched by name
	return strings.Contains(strings.ToLower(object.ObjectName()), strings.ToLower(str))
}
