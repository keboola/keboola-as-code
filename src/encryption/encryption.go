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

type Record struct {
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

func (f *finder) FindValues(projectState *state.State) []Record {
	var records []Record
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
			records = append(records, Record{object, f.values})
		}
	}
	return records
}
func FindUnencrypted(projectState *state.State) ([]Record, error) {
	f := &finder{utils.NewMultiError(), nil}
	records := f.FindValues(projectState)
	return records, f.errors.ErrorOrNil()
}

func LogRecords(records []Record, logger *zap.SugaredLogger) {
	if len(records) == 0 {
		logger.Info("No values to encrypt.")
		return
	}
	logger.Info("Values to encrypt:")

	for _, record := range records {
		logger.Infof("%v %v", record.object.Kind().Abbr, record.object.RelativePath())
		for _, value := range record.values {
			logger.Infof("  %v", value.keyPath)
		}
	}
}
