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

type SlicesActive struct {
	slices
}

type SlicesArchived struct {
	slices
}

type SlicesInReceiver struct {
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

func (v Slices) AllActive() SlicesActive {
	return SlicesActive{slices: v.slices.Add(slicestate.AllActive.String())}
}

func (v Slices) AllArchived() SlicesActive {
	return SlicesActive{slices: v.slices.Add(slicestate.AllArchived.String())}
}

func (v Slices) AllOpened() SlicesActive {
	return SlicesActive{slices: v.slices.Add(slicestate.AllOpened.String())}
}

func (v Slices) AllClosed() SlicesActive {
	return SlicesActive{slices: v.slices.Add(slicestate.AllClosed.String())}
}

func (v Slices) AllSuccessful() SlicesActive {
	return SlicesActive{slices: v.slices.Add(slicestate.AllSuccessful.String())}
}

func (v Slices) InState(state slicestate.State) SlicesInAState {
	return SlicesInAState{slices: v.slices.Add(state.String())}
}

func (v Slices) Writing() SlicesInAState {
	return v.InState(slicestate.Writing)
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

func (v Slices) Imported() SlicesInAState {
	return v.InState(slicestate.Imported)
}

func (v SlicesInAState) ByKey(k storeKey.SliceKey) KeyT[model.Slice] {
	return v.Key(k.String())
}

func (v SlicesInAState) InReceiver(k storeKey.ReceiverKey) SlicesInReceiver {
	return SlicesInReceiver{slices: v.slices.Add(k.String())}
}

func (v SlicesInAState) InExport(k storeKey.ExportKey) SlicesInExport {
	return SlicesInExport{slices: v.slices.Add(k.String())}
}

func (v SlicesInAState) InFile(k storeKey.FileKey) SlicesInFile {
	return SlicesInFile{slices: v.Add(k.String())}
}
