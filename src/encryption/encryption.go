package encryption

import (
	"keboola-as-code/src/model"
	"keboola-as-code/src/state"
	"regexp"
	"strconv"
	"strings"

	"github.com/iancoleman/orderedmap"
	"go.uber.org/zap"
)

func isKeyToEncrypt(key string) bool {
	return strings.HasPrefix(key, "#")
}

func isEncrypted(value string) bool {
	return regexp.MustCompile(`^KBC::(ProjectSecure|ComponentSecure|ConfigSecure)::.+$`).MatchString(value)
}

type Group struct {
	object model.ObjectState // config or configRow state object
	values []Value
}

type Value struct {
	key     string // key e.g. "#myEncryptedValue"
	value   string // value to encrypt
	keyPath path   // path to value from config
}

type finder struct {
	groups []Group
}

func (g *Group) parseArray(array []interface{}, keyPath path) {
	for idx, value := range array {
		g.parseConfigValue(strconv.Itoa(idx), value, append(keyPath, sliceStep(idx)))
	}
}

func (g *Group) parseOrderedMap(content *orderedmap.OrderedMap, keyPath path) {
	for _, key := range content.Keys() {
		mapValue, _ := content.Get(key)
		g.parseConfigValue(key, mapValue, append(keyPath, mapStep(key)))
	}
}

func (g *Group) parseConfigValue(key string, configValue interface{}, keyPath path) {
	switch value := configValue.(type) {
	case orderedmap.OrderedMap:
		g.parseOrderedMap(&value, keyPath)
	case string:
		if isKeyToEncrypt(key) && !isEncrypted(value) {
			g.values = append(g.values, Value{key, value, keyPath})
		}
	case []interface{}:
		g.parseArray(value, keyPath)
	}
}

func (f *finder) FindValues(projectState *state.State) {
	for _, object := range projectState.All() {
		if !object.HasLocalState() {
			continue
		}

		// Walk through configuration nested structure
		group := Group{object, nil}
		switch o := object.(type) {
		case *model.ConfigState:
			group.parseOrderedMap(o.Local.Content, nil)

		case *model.ConfigRowState:
			group.parseOrderedMap(o.Local.Content, nil)
		}

		// Store group if some values found
		if len(group.values) > 0 {
			f.groups = append(f.groups, group)
		}
	}
}
func FindUnencrypted(projectState *state.State) []Group {
	f := &finder{nil}
	f.FindValues(projectState)
	return f.groups
}

func LogGroups(groups []Group, logger *zap.SugaredLogger) {
	if len(groups) == 0 {
		logger.Info("No values to encrypt.")
		return
	}
	logger.Info("Values to encrypt:")

	for _, group := range groups {
		logger.Infof("%v %v", group.object.Kind().Abbr, group.object.RelativePath())
		for _, value := range group.values {
			logger.Infof("  %v", value.keyPath)
		}
	}
}
