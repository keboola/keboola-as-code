package model

import (
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

// Kind - type of the object, branch, config ...
type Kind struct {
	Name string
	Abbr string
}

type Key interface {
	Level() int     // hierarchical level, "1" for branch, "2" for config, ...
	Kind() Kind     // kind of the object: branch, config, ...
	String() string // human-readable description of the object
	ObjectId() string
	ParentKey() (Key, error) // unique key of the parent object
}

type WithKey interface {
	Key() Key
}

type Object interface {
	Key
	Key() Key
	ObjectName() string
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
}

type Objects interface {
	ObjectsSorter
	Add(objects ...Object) error
	AddOrReplace(objects ...Object) error
	MustAdd(objects ...Object)
	Remove(keys ...Key)
	Get(key Key) (Object, bool)
	GetOrNil(key Key) Object
	MustGet(key Key) Object
	All() []Object
	Branches() (branches []*Branch)
	Configs() []*Config
	ConfigsFrom(branch BranchKey) (configs []*Config)
	ConfigsWithRowsFrom(branch BranchKey) (configs []*ConfigWithRows)
	ConfigRows() []*ConfigRow
	ConfigRowsFrom(config ConfigKey) (rows []*ConfigRow)
}

func (k Kind) IsEmpty() bool {
	return k.Name == "" && k.Abbr == ""
}

func (k Kind) String() string {
	return k.Name
}
