package etcdop

type MirrorUpdate struct {
	Header  *Header
	Restart bool
}

type MirrorUpdateChanges[K, V any] struct {
	MirrorUpdate
	Created []MirrorKVPair[K, V]
	Updated []MirrorKVPair[K, V]
	Deleted []MirrorKVPair[K, V]
}

type MirrorKVPair[K, V any] struct {
	Key   K
	Value V
}
