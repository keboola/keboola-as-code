package schema

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/filestate"
	storeKey "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
)

type files = PrefixT[model.File]

type Files struct {
	schema *Schema
	files
}

type FilesInReceiver struct {
	files
}

type FilesInAState struct {
	files
}

type FilesInExport struct {
	files
}

func (v *Schema) Files() Files {
	return Files{schema: v, files: NewTypedPrefix[model.File](
		NewPrefix("file"),
		v.serde,
	)}
}

func (v Files) InState(state filestate.State) FilesInAState {
	return FilesInAState{files: v.files.Add(string(state))}
}

func (v Files) Opened() FilesInAState {
	return v.InState(filestate.Opened)
}

func (v Files) Closing() FilesInAState {
	return v.InState(filestate.Closing)
}

func (v Files) Importing() FilesInAState {
	return v.InState(filestate.Importing)
}

func (v Files) Imported() FilesInAState {
	return v.InState(filestate.Imported)
}

func (v Files) Failed() FilesInAState {
	return v.InState(filestate.Failed)
}

func (v Files) InReceiver(k storeKey.ReceiverKey) FilesInReceiver {
	return FilesInReceiver{files: v.files.Add(k.String())}
}

func (v FilesInAState) ByKey(k storeKey.FileKey) KeyT[model.File] {
	return v.Key(k.String())
}

func (v FilesInAState) InReceiver(k storeKey.ReceiverKey) FilesInReceiver {
	return FilesInReceiver{files: v.files.Add(k.String())}
}

func (v FilesInAState) InExport(k storeKey.ExportKey) FilesInExport {
	return FilesInExport{files: v.files.Add(k.String())}
}
