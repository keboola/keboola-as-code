package relatedpaths

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

type Paths struct {
	baseDir      string
	relatedPaths []string
}

func New(base model.AbsPath) *Paths {
	return &Paths{baseDir: base.String(), relatedPaths: make([]string, 0)}
}

func (p *Paths) All() []string {
	out := make([]string, 0)
	for _, path := range p.relatedPaths {
		// Prefix by dir -> path will be relative to the project dir
		out = append(out, filesystem.Join(p.baseDir, path))
	}
	return out
}

func (p *Paths) Set(paths ...string) {
	p.Clear()
	p.Add(paths...)
}

func (p *Paths) Add(paths ...string) {
	for _, path := range paths {
		if !filesystem.IsFrom(path, p.baseDir) {
			panic(fmt.Errorf(`path "%s" is not from the dir "%s"`, path, p.baseDir))
		}

		relPath, err := filesystem.Rel(p.baseDir, path)
		if err != nil {
			panic(err)
		}

		p.relatedPaths = append(p.relatedPaths, relPath)
	}
}

func (p *Paths) Clear() {
	p.relatedPaths = make([]string, 0)
}

func (p *Paths) Rename(oldPath, newPath string) {
	if !filesystem.IsFrom(oldPath, p.baseDir) {
		panic(fmt.Errorf(`old "%s" is not from the dir "%s"`, oldPath, p.baseDir))
	}
	if !filesystem.IsFrom(newPath, p.baseDir) {
		panic(fmt.Errorf(`new "%s" is not from the dir "%s"`, oldPath, p.baseDir))
	}
	oldRel, err := filesystem.Rel(p.baseDir, oldPath)
	if err != nil {
		panic(err)
	}
	newRel, err := filesystem.Rel(p.baseDir, newPath)
	if err != nil {
		panic(err)
	}

	// Rename all related paths that match old -> new
	for i, path := range p.relatedPaths {
		if path == oldRel {
			p.relatedPaths[i] = newRel
		} else if filesystem.IsFrom(path, oldRel) {
			pathRel, err := filesystem.Rel(oldRel, path)
			if err != nil {
				panic(err)
			}
			p.relatedPaths[i] = filesystem.Join(newRel, pathRel)
		}
	}
}
