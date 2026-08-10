package pagewriter

import (
	"fmt"
)

func renderPlainText(page string, status int) string {
	switch page {
	case "spinner.gohtml":
		return `The application is re-starting. Please wait...`

	case "restart_disabled.gohtml":
		return `The application has been stopped and cannot be restarted automatically.`

	case "error.gohtml":
		// Same wording as the HTML page, so the Streamlit modal and the full
		// page never explain the same failure two different ways.
		c := copyForStatus(status)
		return fmt.Sprintf("%s. %s (status code: %d)", c.Title, c.Guidance, status)

	default:
		return `No additional information is available.`
	}
}
