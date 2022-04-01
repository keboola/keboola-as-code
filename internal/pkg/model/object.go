package model

import (
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

// Kind - type of the object, branch, config ...
type Kind struct {
	Name string
	Abbr string
}

type ObjectLevel int

type Key interface {
	Level() ObjectLevel      // hierarchical level, "1" for branch, "2" for config, ...
	Kind() Kind              // kind of the object: branch, config, ...
	LogicPath() string       // unique identification of the object
	String() string          // human-readable description of the object
	ObjectId() string        // ID of the object
	ParentKey() (Key, error) // unique key of the parent object
}

type WithKey interface {
	Key() Key
}

type WithParentKey interface {
	ParentKey() (Key, error)
}

type ObjectFactory interface {
	NewObject() Object
}

type ObjectManifestFactory interface {
	NewObjectManifest() ObjectManifest
}

type Object interface {
	Key
	Key() Key
}

type ObjectWithContent interface {
	Object
	GetComponentId() ComponentId
	GetContent() *orderedmap.OrderedMap
}

type ObjectWithRelations interface {
	Object
	GetRelations() Relations
	SetRelations(relations Relations)
	AddRelation(relation Relation)
}

type ObjectsSorter interface {
	Less(i, j Key) bool
	String() string
}

type ObjectsReadOnly interface {
	ObjectsSorter
	Get(key Key) (Object, bool)
	GetOrNil(key Key) Object
	MustGet(key Key) Object
	All() []Object
	Branches() (branches []*Branch)
	Configs() []*Config
	ConfigsFrom(branch BranchKey) (configs []*Config)
	ConfigsWithRows() (configs []*ConfigWithRows)
	ConfigsWithRowsFrom(branch BranchKey) (configs []*ConfigWithRows)
	ConfigRows() []*ConfigRow
	ConfigRowsFrom(config ConfigKey) (rows []*ConfigRow)
}

type Objects interface {
	ObjectsReadOnly
	Add(objects ...Object) error
	AddOrReplace(objects ...Object) error
	MustAdd(objects ...Object)
	Remove(keys ...Key)
}

func (k Kind) IsEmpty() bool {
	return k.Name == "" && k.Abbr == ""
}

func (k Kind) String() string {
	return k.Name
}
