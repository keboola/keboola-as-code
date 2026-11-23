package pagewriter

import (
	"net/http"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
)

type AccessDeniedReason string

const (
	AccessDeniedNotInGroups   AccessDeniedReason = "not_in_groups"
	AccessDeniedNoGroupsClaim AccessDeniedReason = "no_groups_claim"
	AccessDeniedNotAllowed    AccessDeniedReason = "not_allowed"
)

// AccessDenied must not name any group: the allowed groups describe the customer's organization.
type AccessDenied struct {
	Email            string
	ProviderName     string
	Reason           AccessDeniedReason
	SwitchAccountURL string
	ExceptionID      string
}

func (r AccessDeniedReason) guidance() string {
	switch r {
	case AccessDeniedNotInGroups:
		return "Your account is not in a group that is allowed to open this application. Ask the person who shared it with you to give your account access."
	case AccessDeniedNoGroupsClaim:
		return "Your identity provider did not send any groups for your account, so your access could not be checked. Ask your IT administrator to send the groups claim to this application."
	default:
		return "Your account is not allowed by the sign-in rules of this application. Ask the person who shared it with you to give your account access."
	}
}

func (pw *Writer) WriteAccessDeniedPage(w http.ResponseWriter, req *http.Request, app *api.AppConfig, page AccessDenied) {
	data := newErrorPageData(app, http.StatusForbidden, "Access denied ("+string(page.Reason)+")", page.ExceptionID)
	data.PageTitle = "Access denied"
	data.Guidance = page.Reason.guidance()
	data.Email = page.Email
	data.ProviderName = page.ProviderName
	data.SwitchAccountURL = page.SwitchAccountURL
	pw.writeErrorPage(w, req, data)
}
