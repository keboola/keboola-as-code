package model

type Filesystem interface {
	ApiName() string // name of the used implementation, for example local, memory, ...
	ProjectDir() string
	Mkdir(path string) error
	Exists(path string) bool
	IsFile(path string) bool
	IsDir(path string) bool
	Copy(src, dst string) error
	CopyForce(src, dst string) error
	Move(src, dst string) error
	MoveForce(src, dst string) error
	Remove(path string) error
	ReadJsonFieldsTo(path, desc string, target interface{}, tag string) (*JsonFile, error)
	ReadJsonMapTo(path, desc string, target interface{}, tag string) (*JsonFile, error)
	ReadFileContentTo(path, desc string, target interface{}, tag string) (*File, error)
	ReadJsonFile(path, desc string) (*JsonFile, error)
	ReadFile(path, desc string) (*File, error)
	WriteFile(file *File) error
	CreateOrUpdateFile(path, desc string, lines []FileLine) (updated bool, err error)
}
