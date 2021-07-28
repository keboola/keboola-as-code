package encryption

import (
	"keboola-as-code/src/state"
	"keboola-as-code/src/utils"
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
	object state.ObjectState // config or configRow state object
	values []Value
}

type Value struct {
	key     string // key e.g. "#myEncryptedValue"
	value   string // value to encrypt
	keyPath path   // path to value from config
}

type finder struct {
	errors *utils.Error
	values []Value
}

func (f *finder) parseArray(array []interface{}, keyPath path) {
	for idx, value := range array {
		f.parseConfigValue(strconv.Itoa(idx), value, append(keyPath, sliceStep(idx)))
	}
}

func (f *finder) parseOrderedMap(content *orderedmap.OrderedMap, keyPath path) {
	for _, key := range content.Keys() {
		mapValue, _ := content.Get(key)
		f.parseConfigValue(key, mapValue, append(keyPath, mapStep(key)))
	}
}

func (f *finder) parseConfigValue(key string, configValue interface{}, keyPath path) {
	switch value := configValue.(type) {
	case orderedmap.OrderedMap:
		f.parseOrderedMap(&value, keyPath)
	case string:
		if isKeyToEncrypt(key) && !isEncrypted(value) {
			f.values = append(f.values, Value{key, value, keyPath})
		}
	case []interface{}:
		f.parseArray(value, keyPath)
	}
}

func (f *finder) FindValues(projectState *state.State) []Group {
	var groups []Group
	for _, object := range projectState.All() {
		f.values = nil
		if !object.HasLocalState() {
			continue
		}
		switch o := object.(type) {
		case *state.ConfigState:
			f.parseOrderedMap(o.Local.Content, nil)

		case *state.ConfigRowState:
			f.parseOrderedMap(o.Local.Content, nil)
		}
		if len(f.values) > 0 {
			groups = append(groups, Group{object, f.values})
		}
	}
	return groups
}
func FindUnencrypted(projectState *state.State) ([]Group, error) {
	f := &finder{utils.NewMultiError(), nil}
	groups := f.FindValues(projectState)
	return groups, f.errors.ErrorOrNil()
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
