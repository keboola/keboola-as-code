package filesystem

type FileMetadata struct {
	metadata map[string]interface{}
}

func NewFileMetadata() *FileMetadata {
	return &FileMetadata{metadata: make(map[string]interface{})}
}

func (f *FileMetadata) Metadata(key string) (interface{}, bool) {
	v, ok := f.metadata[key]
	return v, ok
}

func (f *FileMetadata) MetadataOrNil(key string) interface{} {
	return f.metadata[key]
}

func (f *FileMetadata) HasMetadata(key string) bool {
	_, found := f.metadata[key]
	return found
}

func (f *FileMetadata) AddMetadata(key string, value interface{}) {
	f.metadata[key] = value
}

func (f *FileMetadata) RemoveMetadata(keys ...string) {
	for _, tag := range keys {
		delete(f.metadata, tag)
	}
}
