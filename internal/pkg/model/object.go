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

type ObjectWithChildren struct {
	Object   `diff:"true"`
	Children map[Kind][]*ObjectWithChildren `diff:"true"`
}

type ObjectsReadOnly interface {
	ObjectsSorter
	// Get object from the collection.
	Get(key Key) (Object, bool)
	// GetOrNil object from the collection or returns nil if it is not present.
	GetOrNil(key Key) Object
	// GetWithChildren gets object with all its children in the tree structure.
	GetWithChildren(rootKey Key) (*ObjectWithChildren, bool)
	// MustGet object from the collection otherwise panic occurs.
	MustGet(key Key) Object
	// All gets all objects from the collection.
	All() []Object
	// AllWithChildren gets all core objects with children in the tree structure.
	// If Kind.IsCore() is true (so the object type is: branch, config or config row),
	// then the object is present in the result at the root level.
	// Otherwise, the object (transformation, orchestration, code, phase, ...)  is included under its parent.
	AllWithChildren() []*ObjectWithChildren
	// Branches gets all branches from the collection.
	Branches() (branches []*Branch)
	// Configs gets all configs from the collection.
	Configs() []*Config
	// ConfigsFrom gets all configs from the branch.
	ConfigsFrom(branch BranchKey) (configs []*Config)
	// ConfigsWithRows gets all configs with rows.
	ConfigsWithRows() (configs []*ConfigWithRows)
	// ConfigsWithRowsFrom gets all configs with rows from the branch.
	ConfigsWithRowsFrom(branch BranchKey) (configs []*ConfigWithRows)
	// ConfigRows gets all config rows.
	ConfigRows() []*ConfigRow
	// ConfigRowsFrom gets all config rows from the branch.
	ConfigRowsFrom(config ConfigKey) (rows []*ConfigRow)
}

type Objects interface {
	ObjectsReadOnly
	Add(objects ...Object) error
	AddOrReplace(objects ...Object) error
	MustAdd(objects ...Object)
	Remove(keys ...Key)
}

func (k Kind) IsCore() bool {
	return k.IsBranch() || k.IsConfig() || k.IsConfigRow()
}

func (k Kind) String() string {
	return k.Name
}
