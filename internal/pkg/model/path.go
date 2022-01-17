package model

type AbsPath struct {
	ObjectPath    string `json:"path" validate:"required"`
	parentPath    string
	parentPathSet bool
}

type Paths struct {
	AbsPath
	RelatedPaths []string `json:"-"` // not serialized, slice is generated when the object is loaded
}

func NewAbsPath(parentPath, objectPath string) AbsPath {
	return AbsPath{parentPath: parentPath, parentPathSet: true, ObjectPath: objectPath}
}
