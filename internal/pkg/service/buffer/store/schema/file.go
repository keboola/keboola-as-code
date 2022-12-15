package schema

import (
	"strconv"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/filestate"
	storeKey "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type files = PrefixT[model.File]

type Files struct {
	schema *Schema
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

func (v Files) Closed() FilesInAState {
	return v.InState(filestate.Closed)
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

func (v FilesInAState) ByKey(k storeKey.FileKey) KeyT[model.File] {
	return v.InExport(k.ExportKey).ID(k.FileID)
}

func (v FilesInAState) InExport(k storeKey.ExportKey) FilesInExport {
	if k.ProjectID == 0 {
		panic(errors.New("file projectID cannot be empty"))
	}
	if k.ReceiverID == "" {
		panic(errors.New("file receiverID cannot be empty"))
	}
	if k.ExportID == "" {
		panic(errors.New("file exportID cannot be empty"))
	}
	return FilesInExport{files: v.files.Add(strconv.Itoa(k.ProjectID)).Add(k.ReceiverID).Add(k.ExportID)}
}

func (v FilesInExport) ID(fileID time.Time) KeyT[model.File] {
	if fileID.IsZero() {
		panic(errors.New("file fileID cannot be empty"))
	}
	return v.files.Key(storeKey.FormatTime(fileID))
}
