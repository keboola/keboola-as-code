package twinformat

import (
	"strings"
)

// Source constants for data sources.
const (
	SourceShopify         = "shopify"
	SourceHubspot         = "hubspot"
	SourceSalesforce      = "salesforce"
	SourceZendesk         = "zendesk"
	SourceStripe          = "stripe"
	SourceGoogleAds       = "google-ads"
	SourceFacebookAds     = "facebook-ads"
	SourceLinkedInAds     = "linkedin-ads"
	SourceGoogleSheets    = "google-sheets"
	SourceMySQL           = "mysql"
	SourcePostgreSQL      = "postgresql"
	SourceMSSQL           = "mssql"
	SourceOracle          = "oracle"
	SourceMongoDB         = "mongodb"
	SourceSnowflake       = "snowflake"
	SourceBigQuery        = "bigquery"
	SourceRedshift        = "redshift"
	SourceS3              = "s3"
	SourceGCS             = "gcs"
	SourceAzureBlob       = "azure-blob"
	SourceFTP             = "ftp"
	SourceSFTP            = "sftp"
	SourceHTTP            = "http"
	SourceEmail           = "email"
	SourceSlack           = "slack"
	SourceJira            = "jira"
	SourceAsana           = "asana"
	SourceAirtable        = "airtable"
	SourceNotion          = "notion"
	SourceIntercom        = "intercom"
	SourceMailchimp       = "mailchimp"
	SourceTwilio          = "twilio"
	SourceSegment         = "segment"
	SourceMixpanel        = "mixpanel"
	SourceAmplitude       = "amplitude"
	SourceGA4             = "ga4"
	SourceGoogleAnalytics = "google-analytics"
	SourceTransformed     = "transformed"
	SourceManual          = "manual"
	SourceUnknown         = "unknown"
)

// sourceMapping represents a mapping from bucket name pattern to source.
type sourceMapping struct {
	pattern string
	source  string
}

// getSourceMappings returns the source mappings.
// Order matters - more specific patterns should come first.
func getSourceMappings() []sourceMapping {
	return []sourceMapping{
		// E-commerce
		{"shopify", SourceShopify},
		{"woocommerce", SourceShopify}, // Similar category

		// CRM
		{"hubspot", SourceHubspot},
		{"salesforce", SourceSalesforce},
		{"zendesk", SourceZendesk},
		{"intercom", SourceIntercom},

		// Payments
		{"stripe", SourceStripe},
		{"paypal", SourceStripe}, // Similar category

		// Advertising
		{"google-ads", SourceGoogleAds},
		{"googleads", SourceGoogleAds},
		{"adwords", SourceGoogleAds},
		{"facebook-ads", SourceFacebookAds},
		{"facebookads", SourceFacebookAds},
		{"fb-ads", SourceFacebookAds},
		{"linkedin-ads", SourceLinkedInAds},
		{"linkedinads", SourceLinkedInAds},

		// Google Services
		{"google-sheets", SourceGoogleSheets},
		{"googlesheets", SourceGoogleSheets},
		{"gsheet", SourceGoogleSheets},
		{"ga4", SourceGA4},
		{"google-analytics", SourceGoogleAnalytics},
		{"googleanalytics", SourceGoogleAnalytics},

		// Databases
		{"mysql", SourceMySQL},
		{"postgresql", SourcePostgreSQL},
		{"postgres", SourcePostgreSQL},
		{"mssql", SourceMSSQL},
		{"sqlserver", SourceMSSQL},
		{"oracle", SourceOracle},
		{"mongodb", SourceMongoDB},
		{"mongo", SourceMongoDB},

		// Data Warehouses
		{"snowflake", SourceSnowflake},
		{"bigquery", SourceBigQuery},
		{"redshift", SourceRedshift},

		// Cloud Storage
		{"s3", SourceS3},
		{"aws-s3", SourceS3},
		{"gcs", SourceGCS},
		{"google-cloud-storage", SourceGCS},
		{"azure-blob", SourceAzureBlob},
		{"azureblob", SourceAzureBlob},

		// File Transfer
		{"ftp", SourceFTP},
		{"sftp", SourceSFTP},
		{"http", SourceHTTP},

		// Communication
		{"email", SourceEmail},
		{"slack", SourceSlack},
		{"mailchimp", SourceMailchimp},
		{"twilio", SourceTwilio},

		// Project Management
		{"jira", SourceJira},
		{"asana", SourceAsana},
		{"airtable", SourceAirtable},
		{"notion", SourceNotion},

		// Analytics
		{"segment", SourceSegment},
		{"mixpanel", SourceMixpanel},
		{"amplitude", SourceAmplitude},

		// Keboola-specific patterns (note: "out.c-" is handled separately via early return)
		{"processed", SourceTransformed},
		{"transformed", SourceTransformed},
		{"staging", SourceTransformed},
		{"manual", SourceManual},
		{"upload", SourceManual},
	}
}

// InferSourceFromBucket infers the data source from a bucket name.
// Returns the source name (e.g., "shopify", "hubspot", "transformed").
func InferSourceFromBucket(bucketName string) string {
	if bucketName == "" {
		return SourceUnknown
	}

	bucketNameLower := strings.ToLower(bucketName)

	// Check for output bucket pattern first (out.c-*)
	if strings.HasPrefix(bucketNameLower, "out.c-") {
		return SourceTransformed
	}

	// Remove common prefixes for better matching
	cleanName := bucketNameLower
	cleanName = strings.TrimPrefix(cleanName, "in.c-")
	cleanName = strings.TrimPrefix(cleanName, "out.c-")

	for _, mapping := range getSourceMappings() {
		if strings.Contains(cleanName, mapping.pattern) {
			return mapping.source
		}
	}

	return SourceUnknown
}

// InferSourceFromTableID infers the data source from a full table ID.
// Table ID format: "stage.bucket.table" (e.g., "in.c-shopify.orders").
func InferSourceFromTableID(tableID string) string {
	if tableID == "" {
		return SourceUnknown
	}

	// Parse table ID to extract bucket
	parts := strings.Split(tableID, ".")
	if len(parts) < 2 {
		return SourceUnknown
	}

	// Reconstruct bucket name (first two parts)
	bucketName := strings.Join(parts[:2], ".")

	return InferSourceFromBucket(bucketName)
}

// GetSourceDisplayName returns a human-readable display name for a source.
func GetSourceDisplayName(source string) string {
	switch source {
	case SourceShopify:
		return "Shopify"
	case SourceHubspot:
		return "HubSpot"
	case SourceSalesforce:
		return "Salesforce"
	case SourceZendesk:
		return "Zendesk"
	case SourceStripe:
		return "Stripe"
	case SourceGoogleAds:
		return "Google Ads"
	case SourceFacebookAds:
		return "Facebook Ads"
	case SourceLinkedInAds:
		return "LinkedIn Ads"
	case SourceGoogleSheets:
		return "Google Sheets"
	case SourceMySQL:
		return "MySQL"
	case SourcePostgreSQL:
		return "PostgreSQL"
	case SourceMSSQL:
		return "MS SQL Server"
	case SourceOracle:
		return "Oracle"
	case SourceMongoDB:
		return "MongoDB"
	case SourceSnowflake:
		return "Snowflake"
	case SourceBigQuery:
		return "BigQuery"
	case SourceRedshift:
		return "Redshift"
	case SourceS3:
		return "Amazon S3"
	case SourceGCS:
		return "Google Cloud Storage"
	case SourceAzureBlob:
		return "Azure Blob Storage"
	case SourceFTP:
		return "FTP"
	case SourceSFTP:
		return "SFTP"
	case SourceHTTP:
		return "HTTP"
	case SourceEmail:
		return "Email"
	case SourceSlack:
		return "Slack"
	case SourceJira:
		return "Jira"
	case SourceAsana:
		return "Asana"
	case SourceAirtable:
		return "Airtable"
	case SourceNotion:
		return "Notion"
	case SourceIntercom:
		return "Intercom"
	case SourceMailchimp:
		return "Mailchimp"
	case SourceTwilio:
		return "Twilio"
	case SourceSegment:
		return "Segment"
	case SourceMixpanel:
		return "Mixpanel"
	case SourceAmplitude:
		return "Amplitude"
	case SourceGA4:
		return "Google Analytics 4"
	case SourceGoogleAnalytics:
		return "Google Analytics"
	case SourceTransformed:
		return "Transformed"
	case SourceManual:
		return "Manual Upload"
	default:
		return "Unknown"
	}
}

// GetSourceType returns the type category for a source.
func GetSourceType(source string) string {
	switch source {
	case SourceShopify:
		return "e-commerce"
	case SourceHubspot, SourceSalesforce, SourceZendesk, SourceIntercom:
		return "crm"
	case SourceStripe:
		return "payments"
	case SourceGoogleAds, SourceFacebookAds, SourceLinkedInAds:
		return "advertising"
	case SourceGoogleSheets, SourceAirtable, SourceNotion:
		return "productivity"
	case SourceMySQL, SourcePostgreSQL, SourceMSSQL, SourceOracle, SourceMongoDB:
		return "database"
	case SourceSnowflake, SourceBigQuery, SourceRedshift:
		return "data-warehouse"
	case SourceS3, SourceGCS, SourceAzureBlob:
		return "cloud-storage"
	case SourceFTP, SourceSFTP, SourceHTTP:
		return "file-transfer"
	case SourceEmail, SourceSlack, SourceMailchimp, SourceTwilio:
		return "communication"
	case SourceJira, SourceAsana:
		return "project-management"
	case SourceSegment, SourceMixpanel, SourceAmplitude, SourceGA4, SourceGoogleAnalytics:
		return "analytics"
	case SourceTransformed:
		return "transformed"
	case SourceManual:
		return "manual"
	default:
		return "unknown"
	}
}
