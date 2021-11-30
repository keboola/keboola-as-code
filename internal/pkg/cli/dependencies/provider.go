package dependencies

type Provider interface {
	Dependencies() *Container
}
