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
	return regexp.MustCompile(`^KBC::ProjectSecure::.+$`).MatchString(value)
}

type Record struct {
	object  state.ObjectState // config or configRow state object
	key     string            // key e.g. "#myEncryptedValue"
	value   string            // value to encrypt
	keyPath path              // path to value from config
}

type finder struct {
	errors *utils.Error
	values []Record
}

func (f *finder) parseArray(object state.ObjectState, array []interface{}, keyPath path) {
	for idx, value := range array {
		f.parseConfigValue(object, strconv.Itoa(idx), value, append(keyPath, sliceStep(idx)))
	}

}

func (f *finder) parseOrderedMap(object state.ObjectState, content *orderedmap.OrderedMap, keyPath path) {
	for _, key := range content.Keys() {
		mapValue, _ := content.Get(key)
		f.parseConfigValue(object, key, mapValue, append(keyPath, mapStep(key)))
	}
}

func (f *finder) parseConfigValue(object state.ObjectState, key string, configValue interface{}, keyPath path) {
	switch value := configValue.(type) {
	case orderedmap.OrderedMap:
		f.parseOrderedMap(object, &value, keyPath)
	case string:
		if isKeyToEncrypt(key) && !isEncrypted(value) {
			f.values = append(f.values, Record{object, key, value, keyPath})
		}
	case []interface{}:
		f.parseArray(object, value, keyPath)
	}
}

func (f *finder) FindValues(projectState *state.State) {
	for _, object := range projectState.All() {
		if !object.HasLocalState() {
			continue
		}
		switch o := object.(type) {
		case *state.ConfigState:
			f.parseOrderedMap(o, o.Local.Content, nil)
		case *state.ConfigRowState:
			f.parseOrderedMap(o, o.Local.Content, nil)
		}
	}
}
func FindUnencrypted(projectState *state.State) ([]Record, error) {
	f := &finder{utils.NewMultiError(), nil}
	f.FindValues(projectState)
	return f.values, f.errors.ErrorOrNil()
}

func LogValues(values []Record, logger *zap.SugaredLogger) {
	if len(values) == 0 {
		logger.Info("No values to encrypt.")
	} else {
		logger.Info("Values to encrypt:")
		previousObjectId := ""
		for _, value := range values {
			if previousObjectId != value.object.ObjectId() {
				logger.Infof("%v %v", value.object.Kind().Abbr, value.object.RelativePath())
				previousObjectId = value.object.ObjectId()
			}

			logger.Infof("  %v", value.keyPath)

		}

	}
}
