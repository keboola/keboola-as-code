package create

// type Options struct {
//	Objects model.ObjectStates
//}
//
// type dependencies interface {
//	Telemetry() telemetry.Telemetry
//}

// func Run(ctx context.Context, o Options, d dependencies) (results *diff.Results, err error) {
//	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.template.sync.diff.create")
//	defer span.End(&err)
//
//	differ := diff.NewDiffer(o.Objects)
//	results, err = differ.Diff()
//	if err != nil {
//		return nil, err
//	}
//	return results, nil
//}
