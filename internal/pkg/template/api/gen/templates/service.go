// Code generated by goa v3.5.5, DO NOT EDIT.
//
// templates service
//
// Command:
// $ goa gen github.com/keboola/keboola-as-code/api/templates --output
// ./internal/pkg/template/api

package templates

import (
	"context"

	dependencies "github.com/keboola/keboola-as-code/internal/pkg/template/api/dependencies"
	templatesviews "github.com/keboola/keboola-as-code/internal/pkg/template/api/gen/templates/views"
	"goa.design/goa/v3/security"
)

// Service for applying templates to Keboola projects
type Service interface {
	// IndexRoot implements index-root.
	IndexRoot(dependencies.Container) (err error)
	// HealthCheck implements health-check.
	HealthCheck(dependencies.Container) (res string, err error)
	// Index implements index.
	IndexEndpoint(dependencies.Container) (res *Index, err error)
	// Foo implements foo.
	Foo(dependencies.Container, *FooPayload) (res string, err error)
}

// Auther defines the authorization functions to be implemented by the service.
type Auther interface {
	// APIKeyAuth implements the authorization logic for the APIKey security scheme.
	APIKeyAuth(ctx context.Context, key string, schema *security.APIKeyScheme) (context.Context, error)
}

// ServiceName is the name of the service as defined in the design. This is the
// same value that is set in the endpoint request contexts under the ServiceKey
// key.
const ServiceName = "templates"

// MethodNames lists the service method names as defined in the design. These
// are the same values that are set in the endpoint request contexts under the
// MethodKey key.
var MethodNames = [4]string{"index-root", "health-check", "index", "foo"}

// Index is the result type of the templates service index method.
type Index struct {
	// Name of the API
	API string
	// Url of the API documentation
	Documentation string
}

// FooPayload is the payload type of the templates service foo method.
type FooPayload struct {
	StorageAPIToken string
}

// NewIndex initializes result type Index from viewed result type Index.
func NewIndex(vres *templatesviews.Index) *Index {
	return newIndex(vres.Projected)
}

// NewViewedIndex initializes viewed result type Index from result type Index
// using the given view.
func NewViewedIndex(res *Index, view string) *templatesviews.Index {
	p := newIndexView(res)
	return &templatesviews.Index{Projected: p, View: "default"}
}

// newIndex converts projected type Index to service type Index.
func newIndex(vres *templatesviews.IndexView) *Index {
	res := &Index{}
	if vres.API != nil {
		res.API = *vres.API
	}
	if vres.Documentation != nil {
		res.Documentation = *vres.Documentation
	}
	return res
}

// newIndexView projects result type Index to projected type IndexView using
// the "default" view.
func newIndexView(res *Index) *templatesviews.IndexView {
	vres := &templatesviews.IndexView{
		API:           &res.API,
		Documentation: &res.Documentation,
	}
	return vres
}
