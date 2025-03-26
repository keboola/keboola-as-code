package config

import (
	"bytes"
	"net/url"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Repositories []model.TemplateRepository

func DefaultRepositories() Repositories {
	return Repositories{
		{
			Type: model.RepositoryTypeGit,
			Name: repository.DefaultTemplateRepositoryName,
			URL:  repository.DefaultTemplateRepositoryURL,
			Ref:  repository.DefaultTemplateRepositoryRefMain,
		},
		{
			Type: model.RepositoryTypeGit,
			Name: repository.DefaultTemplateRepositoryNameBeta,
			URL:  repository.DefaultTemplateRepositoryURL,
			Ref:  repository.DefaultTemplateRepositoryRefBeta,
		},
		{
			Type: model.RepositoryTypeGit,
			Name: repository.DefaultTemplateRepositoryNameDev,
			URL:  repository.DefaultTemplateRepositoryURL,
			Ref:  repository.DefaultTemplateRepositoryRefDev,
		},
		{
			Type: model.RepositoryTypeGit,
			Name: repository.ComponentsTemplateRepositoryName,
			URL:  repository.ComponentsTemplateRepositoryURL,
			Ref:  repository.DefaultTemplateRepositoryRefMain,
		},
		{
			Type: model.RepositoryTypeGit,
			Name: repository.ComponentsTemplateRepositoryNameBeta,
			URL:  repository.ComponentsTemplateRepositoryURL,
			Ref:  repository.DefaultTemplateRepositoryRefBeta,
		},
	}
}

func (r Repositories) MarshalText() ([]byte, error) {
	var out bytes.Buffer
	for i, repo := range r {
		if i != 0 {
			out.WriteString(";")
		}
		switch repo.Type {
		case model.RepositoryTypeGit:
			out.WriteString(repo.Name)
			out.WriteString("|")
			out.WriteString(repo.URL)
			out.WriteString("|")
			out.WriteString(repo.Ref)
		case model.RepositoryTypeDir:
			out.WriteString(repo.Name)
			out.WriteString("|")
			out.WriteString("file://")
			out.WriteString(repo.URL)
		default:
			panic(errors.Errorf(`unexpected repo.Type value "%v"`, repo.Type))
		}
	}
	return out.Bytes(), nil
}

func (r *Repositories) UnmarshalText(inBytes []byte) error {
	in := string(inBytes)
	in = strings.TrimSpace(in)
	if len(in) == 0 {
		return nil
	}

	out := Repositories{}

	// Definitions are separated by ";"
	usedNames := make(map[string]bool)
	for definition := range strings.SplitSeq(in, ";") {
		// Definition parts are separated by "|"
		parts := strings.Split(definition, "|")
		if len(parts) < 2 {
			return errors.Errorf(`invalid repository definition "%s": required format <name>|https://<repository>|<branch> or <name>|file://<repository>`, definition)
		}
		name := parts[0]
		path := parts[1]

		// Each default repository must have unique name
		if usedNames[name] {
			return errors.Errorf(`duplicate repository name "%s" found when parsing default repositories`, name)
		}
		usedNames[name] = true

		switch {
		case strings.HasPrefix(path, "file://"):
			if len(parts) != 2 {
				return errors.Errorf(`invalid repository definition "%s": required format <name>|file://<repository>`, definition)
			}
			out = append(out, model.TemplateRepository{
				Type: model.RepositoryTypeDir,
				Name: name,
				URL:  strings.TrimPrefix(path, "file://"),
			})
		case strings.HasPrefix(path, "https://"):
			if len(parts) != 3 {
				return errors.Errorf(`invalid repository definition "%s": required format <name>|https://<repository>|<branch>`, definition)
			}
			if _, err := url.ParseRequestURI(path); err != nil {
				return errors.Errorf(`invalid repository url "%s": %w`, path, err)
			}
			out = append(out, model.TemplateRepository{
				Type: model.RepositoryTypeGit,
				Name: name,
				URL:  path,
				Ref:  parts[2],
			})
		default:
			return errors.Errorf(`invalid repository path "%s": must start with "file://" or "https://"`, path)
		}
	}

	*r = out
	return nil
}
