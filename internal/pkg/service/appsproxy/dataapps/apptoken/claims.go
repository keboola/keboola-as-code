package apptoken

import (
	sdk "github.com/keboola/keboola-sdk-go/v2/pkg/apptoken"
)

// HeaderName is the header in which the token is passed on. The header, the claims and the rest of the contract with the services
// that verify the token are in keboola-sdk-go, which they share.
const HeaderName = sdk.HeaderName
