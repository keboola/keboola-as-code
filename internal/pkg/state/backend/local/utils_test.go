package local

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testfs"
)

func TestDeleteEmptyDirectories(t *testing.T) {
	t.Parallel()
	fs := testfs.NewMemoryFs()

	// Structure:
	// D .hidden
	// D .git
	//     D empty
	// D tracked-empty
	// D tracked-empty-sub
	//     D abc
	// D non-tracked-empty
	// D tracked
	//    F foo.txt
	// D non-tracked
	//    F foo.txt
	// D tracked-with-hidden
	//    D .git

	// Create structure
	assert.NoError(t, fs.Mkdir(`.hidden`))
	assert.NoError(t, fs.Mkdir(filesystem.Join(`.git`, `empty`)))
	assert.NoError(t, fs.Mkdir(`tracked-empty`))
	assert.NoError(t, fs.Mkdir(filesystem.Join(`tracked-empty-sub`, `abc`)))
	assert.NoError(t, fs.Mkdir(`non-tracked-empty`))
	assert.NoError(t, fs.Mkdir(`tracked`))
	assert.NoError(t, fs.Mkdir(`non-tracked`))
	assert.NoError(t, fs.Mkdir(filesystem.Join(`tracked-with-hidden`, `.git`)))
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(filesystem.Join(`tracked`, `foo.txt`), `bar`)))
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(filesystem.Join(`non-tracked`, `foo.txt`), `bar`)))

	// Delete
	trackedPaths := []string{
		`.hidden`,
		`tracked-empty`,
		`tracked-empty-sub`,
		`tracked`,
		`tracked-with-hidden`,
	}
	assert.NoError(t, deleteEmptyDirectories(fs, trackedPaths))

	// Assert
	assert.False(t, fs.Exists(`tracked-empty`))
	assert.False(t, fs.Exists(`tracked-empty-sub`))

	assert.True(t, fs.Exists(`.hidden`))
	assert.True(t, fs.Exists(filesystem.Join(`.git`, `empty`)))
	assert.True(t, fs.Exists(`non-tracked-empty`))
	assert.True(t, fs.Exists(filesystem.Join(`tracked-with-hidden`, `.git`)))
	assert.True(t, fs.Exists(filesystem.Join(`tracked`, `foo.txt`)))
	assert.True(t, fs.Exists(filesystem.Join(`non-tracked`, `foo.txt`)))
}

func TestUnitOfWork_workersFor(t *testing.T) {
	t.Parallel()
	u := &uow{workers: orderedmap.New()}

	lock := &sync.Mutex{}
	var order []model.ObjectLevel

	for _, level := range []model.ObjectLevel{3, 2, 4, 1} {
		level := level
		u.workersFor(level).AddWorker(func() error {
			lock.Lock()
			defer lock.Unlock()
			order = append(order, level)
			return nil
		})
		u.workersFor(level).AddWorker(func() error {
			lock.Lock()
			defer lock.Unlock()
			order = append(order, level)
			return nil
		})
	}

	// Not started
	time.Sleep(10 * time.Millisecond)
	assert.Empty(t, order)

	// Invoke
	_, err := u.Invoke()
	assert.NoError(t, err)
	assert.Equal(t, []model.ObjectLevel{1, 1, 2, 2, 3, 3, 4, 4}, order)

	// Cannot be reused
	assert.PanicsWithError(t, `invoked local.UnitOfWork cannot be reused`, func() {
		u.Invoke()
	})
}
