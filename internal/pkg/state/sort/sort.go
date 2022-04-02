package sort

import (
	"fmt"

	"golang.org/x/text/collate"
	"golang.org/x/text/language"

	. "github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local/naming"
)

const (
	idSorterName   = "id"
	pathSorterName = "path"
)

type idSorter struct {
	comparator *collate.Collator
}

type pathSorter struct {
	naming     *naming.Registry
	comparator *collate.Collator
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
	return idSorter{
		comparator: collate.New(language.Make("en-US"), collate.Numeric),
	}
}

// NewPathSorter - sort objects by level and filesystem paths.
func NewPathSorter(naming *naming.Registry) ObjectsSorter {
	return pathSorter{
		naming:     naming,
		comparator: collate.New(language.Make("en-US"), collate.Numeric, collate.Loose),
	}
}

func (s idSorter) Less(i, j Key) bool {
	if levelDiff := i.Level() - j.Level(); levelDiff != 0 {
		// Different levels -> sort by level
		return levelDiff < 0
	} else {
		// Same level -> sort by IDs
		return s.comparator.CompareString(i.LogicPath(), j.LogicPath()) < 0
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
			return s.comparator.CompareString(iPath.String(), jPath.String()) < 0
		} else {
			// Fallback -> sort by IDs
			// Same level -> sort by ID
			return s.comparator.CompareString(i.LogicPath(), j.LogicPath()) < 0
		}
	}
}

func (s pathSorter) String() string {
	return pathSorterName
}
