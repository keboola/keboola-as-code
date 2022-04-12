package corefiles

// coreFilesMapper performs local loading / saving of files: config.json, meta.json, description.md.
type coreFilesMapper struct{}

func NewLocalMapper() *coreFilesMapper {
	return &coreFilesMapper{}
}
