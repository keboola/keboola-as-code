package slice

//
//// Rotate closes the opened slice, if present, and opens a new slice in the file volume.
////   - THE NEW SLICE is ALWAYS created in the state storage.SliceWriting.
////   - THE OLD SLICE in the storage.SliceWriting state, IF PRESENT, is switched to the storage.SliceClosing state.
////   - If no old slice exists, this operation effectively corresponds to the Open operation.
////   - Slices rotation is done atomically.
////   - This method is used to rotate slices when the upload conditions are met.
//func (r *Repository) Rotate(now time.Time, k model.FileVolumeKey) *op.AtomicOp[model.Slice] {
//	// Init atomic operation
//	var file model.File
//	var slices []model.Slice
//	var newSlice model.Slice
//	return op.Atomic(r.client, &newSlice).
//		// Load file
//		ReadOp(r.files.Get(k.FileKey).WithResultTo(&file)).
//		// Load slices
//		ReadOp(r.ListInState(k, model.SliceWriting).WithAllTo(&slices)).
//		// Close old slice, open a new one, if enabled
//		WriteOrErr(func(ctx context.Context) (op.Op, error) {
//			saveCtx := plugin.NewSaveContext(now)
//
//			// Close old slice, if any
//			if err := r.closeSlices(saveCtx, slices); err != nil {
//				return nil, err
//			}
//
//			// Open new slice
//			if slice, err := r.openSlice(saveCtx, file, k.VolumeID); err == nil {
//				newSlice = slice
//			} else {
//				return nil, err
//			}
//
//			return saveCtx.Do(ctx)
//		})
//}
