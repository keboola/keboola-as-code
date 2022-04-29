// Package metadata handles serialization of the template metadata to config metadata.
package metadata

const (
	repositoryKey = "KBC.KAC.templates.repository"
	templateIdKey = "KBC.KAC.templates.templateId"
	instanceIdKey = "KBC.KAC.templates.instanceId" // attach config to a template instance
)

// ConfigMetadata stores config template metadata to config metadata.
type ConfigMetadata map[string]string

func (m ConfigMetadata) Repository() string {
	return m[repositoryKey]
}

func (m ConfigMetadata) SetRepository(repository string) {
	m[repositoryKey] = repository
}

func (m ConfigMetadata) TemplateId() string {
	return m[templateIdKey]
}

func (m ConfigMetadata) SetTemplateId(templateId string) {
	m[templateIdKey] = templateId
}

func (m ConfigMetadata) InstanceId() string {
	return m[instanceIdKey]
}

func (m ConfigMetadata) SetInstanceId(instanceId string) {
	m[instanceIdKey] = instanceId
}
