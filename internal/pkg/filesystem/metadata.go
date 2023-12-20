package filesystem

type FileMetadata struct {
	metadata map[string]any
}

func NewFileMetadata() *FileMetadata {
	return &FileMetadata{metadata: make(map[string]any)}
}

func (f *FileMetadata) Metadata(key string) (any, bool) {
	v, ok := f.metadata[key]
	return v, ok
}

func (f *FileMetadata) MetadataOrNil(key string) any {
	return f.metadata[key]
}

func (f *FileMetadata) HasMetadata(key string) bool {
	_, found := f.metadata[key]
	return found
}

func (f *FileMetadata) AddMetadata(key string, value any) {
	f.metadata[key] = value
}

func (f *FileMetadata) RemoveMetadata(keys ...string) {
	for _, tag := range keys {
		delete(f.metadata, tag)
	}
}
