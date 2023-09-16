package volume

// Info is a container for volume metadata.
type Info struct {
	path  string
	typ   string
	label string
}

func NewInfo(path, typ, label string) Info {
	return Info{path: path, typ: typ, label: label}
}

func (m *Info) Path() string {
	return m.path
}

func (m *Info) Type() string {
	return m.typ
}

func (m *Info) Label() string {
	return m.label
}
