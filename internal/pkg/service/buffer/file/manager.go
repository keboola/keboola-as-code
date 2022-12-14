package file

import (
	"context"
	"fmt"
	"sync"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/storageapi"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
)

type Manager struct {
	storageClient client.Sender
}

func NewManager(storageClient client.Sender) *Manager {
	return &Manager{storageClient: storageClient}
}

func (m *Manager) CreateFile(ctx context.Context, name string) (*storageapi.File, error) {
	return storageapi.CreateFileResourceRequest(&storageapi.File{
		Name:     name,
		IsSliced: true,
	}).Send(ctx, m.storageClient)
}

func (m *Manager) CreateFilesForReceiver(ctx context.Context, receiver *model.Receiver) (map[key.ExportKey]*storageapi.File, error) {
	mutex := &sync.RWMutex{}
	files := make(map[key.ExportKey]*storageapi.File)
	wg := client.NewWaitGroup(ctx, m.storageClient)
	for _, export := range receiver.Exports {
		expKey := export.ExportKey
		wg.Send(
			storageapi.CreateFileResourceRequest(&storageapi.File{
				Name:     export.Name,
				IsSliced: true,
			}).WithOnSuccess(func(ctx context.Context, sender client.Sender, result *storageapi.File) error {
				mutex.Lock()
				files[expKey] = result
				mutex.Unlock()
				return nil
			}),
		)
	}
	if err := wg.Wait(); err != nil {
		return nil, err
	}
	return files, nil
}

func (m *Manager) UploadSlicedFileManifest(ctx context.Context, file *model.File, slices []*model.Slice) {
	sliceFiles := make([]string, 0)
	for _, s := range slices {
		sliceFiles = append(sliceFiles, sliceNumberToFilename(s.SliceNumber))
	}
	w, err := storageapi.UploadSlicedFileManifest(ctx, file.StorageResource, sliceFiles)
}

func sliceNumberToFilename(n int) string {
	return fmt.Sprintf("slice_%d", n)
}
