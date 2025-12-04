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
		return fmt.Sprintf("An error occurred (status code: %d). Please try again later.", status)

	default:
		return `No additional information is available.`
	}
}
