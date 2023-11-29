package storage

// FileWriting
// It is the initial state of the File.
// API nodes writes records to the File slices in the local volumes.
const FileWriting FileState = "writing"

// FileClosing
// Import conditions have been met.
// Waiting for all slices to be in the SliceUploaded state.
const FileClosing FileState = "closing"

// FileImporting
// The coordinator is/will be importing the File using a Storage Job.
const FileImporting FileState = "importing"

// FileImported
// The File has been successfully imported.
const FileImported FileState = "imported"

type FileState string
