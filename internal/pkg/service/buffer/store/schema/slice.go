package schema

import (
	storeKey "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/slicestate"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
)

type slices = PrefixT[model.Slice]

type Slices struct {
	schema *Schema
	slices
}

type SlicesInAState struct {
	slices
}

type SlicesInExport struct {
	slices
}

type SlicesInFile struct {
	slices
}

func (v *Schema) Slices() Slices {
	return Slices{schema: v, slices: NewTypedPrefix[model.Slice](
		NewPrefix("slice"),
		v.serde,
	)}
}

func (v Slices) InState(state slicestate.State) SlicesInAState {
	return SlicesInAState{slices: v.slices.Add(string(state))}
}

func (v Slices) Opened() SlicesInAState {
	return v.InState(slicestate.Opened)
}

func (v Slices) Closing() SlicesInAState {
	return v.InState(slicestate.Closing)
}

func (v Slices) Uploading() SlicesInAState {
	return v.InState(slicestate.Uploading)
}

func (v Slices) Uploaded() SlicesInAState {
	return v.InState(slicestate.Uploaded)
}

func (v Slices) Failed() SlicesInAState {
	return v.InState(slicestate.Failed)
}

func (v SlicesInAState) ByKey(k storeKey.SliceKey) KeyT[model.Slice] {
	return v.Key(k.String())
}

func (v SlicesInAState) InExport(k storeKey.ExportKey) SlicesInExport {
	return SlicesInExport{slices: v.slices.Add(k.String())}
}

func (v SlicesInAState) InFile(k storeKey.FileKey) SlicesInFile {
	return SlicesInFile{slices: v.Add(k.String())}
}
