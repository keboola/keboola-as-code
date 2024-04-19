package pagewriter

import "github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"

type AppData struct {
	ProjectID string
	ID        string
	Name      string
	IDAndName string
}

func NewAppData(app *api.AppConfig) AppData {
	return AppData{
		ProjectID: app.ProjectID,
		ID:        app.ID.String(),
		Name:      app.Name,
		IDAndName: app.IdAndName(),
	}
}
