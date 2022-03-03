// Package metadata handles serialization of the template metadata to config metadata.
package metadata

const (
	repositoryKey = "KBC.KAC.templates.repository"
	templateIdKey = "KBC.KAC.templates.templateId"
	instanceIdKey = "KBC.KAC.templates.instanceId" // attach config to a template instance
)

// configMetadata stores config template metadata to config metadata.
type configMetadata struct {
	data map[string]string
}

// rowMetadata stores row template metadata to config metadata.
type rowMetadata struct {
	rowTemplateId string
	data          map[string]string
}

func ConfigMetadata(data map[string]string) *configMetadata {
	return &configMetadata{data: data}
}

func (m *configMetadata) Repository() string {
	return m.data[repositoryKey]
}

func (m *configMetadata) SetRepository(repository string) *configMetadata {
	m.data[repositoryKey] = repository
	return m
}

func (m *configMetadata) TemplateId() string {
	return m.data[templateIdKey]
}

func (m *configMetadata) SetTemplateId(templateId string) *configMetadata {
	m.data[templateIdKey] = templateId
	return m
}

func (m *configMetadata) InstanceId() string {
	return m.data[instanceIdKey]
}

func (m *configMetadata) SetInstanceId(instanceId string) *configMetadata {
	m.data[instanceIdKey] = instanceId
	return m
}

func (m *configMetadata) RowMetadata(rowTemplateId string) *rowMetadata {
	return &rowMetadata{rowTemplateId: rowTemplateId, data: m.data}
}
