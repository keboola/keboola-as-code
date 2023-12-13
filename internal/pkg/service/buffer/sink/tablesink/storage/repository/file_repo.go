
// newFile creates file definition.
func newFile(now time.Time, cfg storage.Config, sinkKey key.SinkKey, mapping definition.TableMapping, credentials *keboola.FileUploadCredentials) (f storage.File, err error) {
	// Validate compression type.
	// Other parts of the system are also prepared for other types of compression,
	// but now only GZIP is supported in the Keboola platform.
	switch cfg.Local.Compression.Type {
	case compression.TypeNone, compression.TypeGZIP: // ok
	default:
		return storage.File{}, errors.Errorf(`file compression type "%s" is not supported`, cfg.Local.Compression.Type)
	}

	// Convert path separator, on Windows
	fileKey := storage.FileKey{SinkKey: sinkKey, FileID: storage.FileID{OpenedAt: utctime.From(now)}}
	fileDir := filepath.FromSlash(fileKey.String()) //nolint:forbidigo

	f.FileKey = fileKey
	f.Type = storage.FileTypeCSV // different file types are not supported now
	f.State = storage.FileWriting
	f.Columns = mapping.Columns
	f.LocalStorage = local.NewFile(cfg.Local, fileDir)
	f.StagingStorage = staging.NewFile(cfg.Staging, f.LocalStorage, credentials)
	f.TargetStorage = target.NewFile(cfg.Target, mapping.TableID, f.StagingStorage)
	return f, nil
}
