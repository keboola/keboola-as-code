package filesystem

type FileTags struct {
	tags map[string]bool
}

func NewFileTags() *FileTags {
	return &FileTags{tags: make(map[string]bool)}
}

func (f *FileTags) AllTags() []string {
	out := make([]string, len(f.tags))
	i := 0
	for tag := range f.tags {
		out[i] = tag
		i++
	}
	return out
}

func (f *FileTags) HasTag(tag string) bool {
	return f.tags[tag]
}

func (f *FileTags) AddTag(tags ...string) {
	for _, tag := range tags {
		f.tags[tag] = true
	}
}

func (f *FileTags) RemoveTag(tags ...string) {
	for _, tag := range tags {
		delete(f.tags, tag)
	}
}
