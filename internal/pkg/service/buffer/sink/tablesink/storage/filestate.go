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
// The File has been successfully imported to the target table.
const FileImported FileState = "imported"

// FileState is an enum type for file states.
//
// Example File and Slice transitions.
//
//      FILE            SLICE1           SLICE2           SLICE3
//  -----------------------------------------------------------------
//  FileWriting      SliceWriting     -------------    --------------
//  FileWriting      SliceClosing     SliceWriting     --------------
//  FileWriting      SliceUploading   SliceWriting     --------------
//  FileWriting      SliceUploaded    SliceWriting     --------------
//  FileWriting      SliceUploaded    SliceClosing     --------------
//  ...
//  FileWriting      SliceUploaded    SliceUploaded    SliceWriting
//  FileClosing      SliceUploaded    SliceUploaded    SliceClosing
//  FileClosing      SliceUploaded    SliceUploaded    SliceUploading
//  FileClosing      SliceUploaded    SliceUploaded    SliceUploaded
//  FileImporting    SliceUploaded    SliceUploaded    SliceUploaded
//  FileImported     SliceImported    SliceImported    SliceImported
type FileState string
