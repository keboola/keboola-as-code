// Package metadata handles serialization of the template metadata to config metadata.
package metadata

const (
	repositoryKey = "KBC.KAC.templates.repository"
	templateIdKey = "KBC.KAC.templates.templateId"
	instanceIdKey = "KBC.KAC.templates.instanceId" // attach config to a template instance
)

// ConfigMetadata stores config template metadata to config metadata.
type ConfigMetadata map[string]string

func (m ConfigMetadata) SetTemplateInstance(repo string, tmpl string, inst string) {
	m[repositoryKey] = repo
	m[templateIdKey] = tmpl
	m[instanceIdKey] = inst
}

func (m ConfigMetadata) Repository() string {
	return m[repositoryKey]
}

func (m ConfigMetadata) TemplateId() string {
	return m[templateIdKey]
}

func (m ConfigMetadata) InstanceId() string {
	return m[instanceIdKey]
}
