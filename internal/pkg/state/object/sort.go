package object

import (
	. "github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
)

type idSorter struct{}

type pathSorter struct {
	naming *naming.Registry
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
	if levelDiff := i.Level() - j.Level(); levelDiff == 0 {
		// Same level -> sort by ID
		return i.ObjectId() < j.ObjectId()
	} else {
		// Different level -> sort by level
		return levelDiff < 0
	}
}

func (s pathSorter) Less(i, j Key) bool {
	if levelDiff := i.Level() - j.Level(); levelDiff == 0 {
		// Same level -> sort by path
		iPath, iFound := s.naming.PathByKey(i)
		jPath, jFound := s.naming.PathByKey(j)
		if iFound && jFound {
			// Paths found  -> sort by path
			return iPath.Path() < jPath.Path()
		} else {
			// Fallback -> sort by IDs
			return i.ObjectId() < j.ObjectId()
		}

	} else {
		// Different level -> sort by level
		return levelDiff < 0
	}
}
