package model

import (
	"context"
	"sort"
	"strings"
	"sync"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// Collection manages volumes in the path, each instance has V type.
// The collection contains all volumes found on a local reader or writer node.
type Collection[V Volume] struct {
	byID   map[ID]V
	byType map[string][]V
}

func NewCollection[V Volume](volumes []V) (*Collection[V], error) {
	collection := &Collection[V]{
		byID:   make(map[ID]V),
		byType: make(map[string][]V),
	}

	// Add volumes
	idCount := make(map[ID]int)
	for _, vol := range volumes {
		id := vol.ID()
		if id == "" {
			return nil, errors.New("volume ID cannot be empty")
		}

		idCount[id]++
		collection.byID[id] = vol
		collection.byType[vol.Type()] = append(collection.byType[vol.Type()], vol)
	}

	// Each volume ID must be unique
	for id, count := range idCount {
		if count > 1 {
			return nil, errors.Errorf(`found %d volumes with the ID "%s"`, count, id)
		}
	}

	// At least one volume must be found
	if len(idCount) == 0 {
		return nil, errors.New("no volume found")
	}

	return collection, nil
}

// Volume returns the volume instance by the ID or an error if it is not found.
func (v *Collection[V]) Volume(id ID) (V, error) {
	if v, ok := v.byID[id]; ok {
		return v, nil
	} else {
		var empty V
		return empty, errors.Errorf(`volume with ID "%s" not found`, id)
	}
}

func (v *Collection[V]) HasVolume(id ID) bool {
	if _, ok := v.byID[id]; ok {
		return true
	}

	return false
}

// VolumeByType returns volumes which match the type.
func (v *Collection[V]) VolumeByType(typ string) (out []V) {
	out = make([]V, len(v.byType[typ]))
	copy(out, v.byType[typ])
	sortVolumes(out)
	return out
}

func (v *Collection[V]) Count() int {
	return len(v.byID)
}

func (v *Collection[V]) All() (out []V) {
	i := 0
	out = make([]V, len(v.byID))
	for _, v := range v.byID {
		out[i] = v
		i++
	}
	sortVolumes(out)
	return out
}

// Close all volumes.
func (v *Collection[V]) Close(ctx context.Context) error {
	errs := errors.NewMultiError()
	wg := &sync.WaitGroup{}
	for _, v := range v.byID {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := v.Close(ctx); err != nil {
				errs.Append(err)
			}
		}()
	}
	wg.Wait()
	return errs.ErrorOrNil()
}

func sortVolumes[V Volume](v []V) {
	sort.SliceStable(v, func(i, j int) bool {
		if v := strings.Compare(v[i].Type(), v[j].Type()); v != 0 {
			return v < 0
		}
		return strings.Compare(v[i].Label(), v[j].Label()) < 0
	})
}
