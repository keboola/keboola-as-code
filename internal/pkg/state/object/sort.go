package object

import (
	"fmt"

	. "github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
)

const (
	idSorterName   = "id"
	pathSorterName = "path"
)

type idSorter struct{}

type pathSorter struct {
	naming *naming.Registry
}

func NewSorterFromName(name string, naming *naming.Registry) ObjectsSorter {
	switch name {
	case idSorterName:
		return NewIdSorter()
	case pathSorterName:
		return NewPathSorter(naming)
	default:
		panic(fmt.Errorf(`unexpeted objects sorter "%s"`, name))
	}
}

// NewIdSorter - sort objects by level and IDs.
func NewIdSorter() ObjectsSorter {
	return idSorter{}
}

// NewPathSorter - sort objects by level and filesystem paths.
func NewPathSorter(naming *naming.Registry) ObjectsSorter {
	return pathSorter{naming: naming}
}

func (idSorter) Less(i, j Key) bool {
	if levelDiff := i.Level() - j.Level(); levelDiff != 0 {
		// Different levels -> sort by level
		return levelDiff < 0
	} else {
		// Same level -> sort by ID
		return fullObjectId(i) < fullObjectId(j)
	}
}

func (idSorter) String() string {
	return idSorterName
}

func (s pathSorter) Less(i, j Key) bool {
	if levelDiff := i.Level() - j.Level(); levelDiff != 0 {
		// Different level -> sort by level
		return levelDiff < 0
	} else {
		// Same level -> sort by path
		iPath, iFound := s.naming.PathByKey(i)
		jPath, jFound := s.naming.PathByKey(j)
		if iFound && jFound {
			// Paths found  -> sort by path
			return iPath.String() < jPath.String()
		} else {
			// Fallback -> sort by IDs
			return fullObjectId(i) < fullObjectId(j)
		}
	}
}

func (s pathSorter) String() string {
	return pathSorterName
}

func fullObjectId(key Key) (fullId string) {
	for {
		fullId = key.ObjectId() + "/" + fullId
		key, _ = key.ParentKey()
		if key == nil {
			break
		}
	}
	return fullId
}
