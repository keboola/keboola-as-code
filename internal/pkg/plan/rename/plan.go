package rename

import (
	"context"
	"fmt"
	"io"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state/local"
)

type Plan struct {
	actions []model.RenameAction
}

type Options struct {
	Cleanup bool
}

type Option func(*Options)

func WithCleanup(v bool) Option { return func(o *Options) { o.Cleanup = v } }

func (p *Plan) Empty() bool {
	return len(p.actions) == 0
}

func (p *Plan) Name() string {
	return "rename"
}

func (p *Plan) Log(w io.Writer) {
	fmt.Fprintf(w, `Plan for "%s" operation:`, p.Name())
	fmt.Fprintln(w)
	if len(p.actions) == 0 {
		fmt.Fprintln(w, "  no paths to rename")
	} else {
		for _, action := range p.actions {
			fmt.Fprintln(w, "  - "+action.String())
		}
	}
}

func (p *Plan) Invoke(ctx context.Context, localManager *local.Manager, opts ...Option) error {
	options := &Options{}
	for _, o := range opts {
		o(options)
	}
	return newRenameExecutor(ctx, localManager, p, *options).invoke()
}
