package storage

// SliceWriting
// It is the initial state of the Slice.
// API node writes records to the local file.
const SliceWriting SliceState = "writing"

// SliceClosing
// Upload conditions have been met.
// Waiting for the API node until it switch to the new Slice.
const SliceClosing SliceState = "closing"

// SliceUploading
// The Slice is ready for upload.
// The worker from the same pod is/will be uploading the Slice.
const SliceUploading SliceState = "uploading"

// SliceUploaded
// The Slice has been successfully uploaded to the staging storage.
// The Slice can be removed from the local storage.
const SliceUploaded SliceState = "uploaded"

// SliceImported
// The parent File has been successfully imported to the target table.
// The Slice can be removed from the staging storage, if needed.
const SliceImported SliceState = "imported"

// SliceState is an enum type for slice states, see also FileState.
type SliceState string
