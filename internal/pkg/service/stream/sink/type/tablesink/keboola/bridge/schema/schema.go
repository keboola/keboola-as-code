package schema

import "github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"

type Schema struct {
	token Token
	file  File
	job   Job
}

func New(s *serde.Serde) Schema {
	return Schema{
		token: forToken(s),
		file:  forFile(s),
		job:   forJob(s),
	}
}

func (s Schema) Token() Token {
	return s.token
}

func (s Schema) File() File {
	return s.file
}

func (s Schema) Job() Job {
	return s.job
}
