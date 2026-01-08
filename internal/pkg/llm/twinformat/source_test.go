package twinformat

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInferSourceFromBucket(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		bucketName string
		expected   string
	}{
		// E-commerce
		{name: "shopify bucket", bucketName: "in.c-shopify", expected: SourceShopify},
		{name: "shopify with suffix", bucketName: "in.c-shopify-orders", expected: SourceShopify},

		// CRM
		{name: "hubspot bucket", bucketName: "in.c-hubspot", expected: SourceHubspot},
		{name: "salesforce bucket", bucketName: "in.c-salesforce", expected: SourceSalesforce},
		{name: "zendesk bucket", bucketName: "in.c-zendesk", expected: SourceZendesk},

		// Payments
		{name: "stripe bucket", bucketName: "in.c-stripe", expected: SourceStripe},

		// Advertising
		{name: "google ads bucket", bucketName: "in.c-google-ads", expected: SourceGoogleAds},
		{name: "googleads bucket", bucketName: "in.c-googleads", expected: SourceGoogleAds},
		{name: "facebook ads bucket", bucketName: "in.c-facebook-ads", expected: SourceFacebookAds},
		{name: "linkedin ads bucket", bucketName: "in.c-linkedin-ads", expected: SourceLinkedInAds},

		// Databases
		{name: "mysql bucket", bucketName: "in.c-mysql", expected: SourceMySQL},
		{name: "postgresql bucket", bucketName: "in.c-postgresql", expected: SourcePostgreSQL},
		{name: "postgres bucket", bucketName: "in.c-postgres", expected: SourcePostgreSQL},
		{name: "mongodb bucket", bucketName: "in.c-mongodb", expected: SourceMongoDB},

		// Data warehouses
		{name: "snowflake bucket", bucketName: "in.c-snowflake", expected: SourceSnowflake},
		{name: "bigquery bucket", bucketName: "in.c-bigquery", expected: SourceBigQuery},

		// Cloud storage
		{name: "s3 bucket", bucketName: "in.c-s3", expected: SourceS3},
		{name: "aws-s3 bucket", bucketName: "in.c-aws-s3", expected: SourceS3},
		{name: "gcs bucket", bucketName: "in.c-gcs", expected: SourceGCS},

		// Output buckets (transformed data)
		{name: "output bucket", bucketName: "out.c-processed", expected: SourceTransformed},
		{name: "output bucket any name", bucketName: "out.c-anything", expected: SourceTransformed},

		// Analytics
		{name: "ga4 bucket", bucketName: "in.c-ga4", expected: SourceGA4},
		{name: "google-analytics bucket", bucketName: "in.c-google-analytics", expected: SourceGoogleAnalytics},
		{name: "segment bucket", bucketName: "in.c-segment", expected: SourceSegment},
		{name: "mixpanel bucket", bucketName: "in.c-mixpanel", expected: SourceMixpanel},

		// Communication
		{name: "slack bucket", bucketName: "in.c-slack", expected: SourceSlack},
		{name: "email bucket", bucketName: "in.c-email", expected: SourceEmail},

		// Manual
		{name: "manual bucket", bucketName: "in.c-manual", expected: SourceManual},
		{name: "upload bucket", bucketName: "in.c-upload", expected: SourceManual},

		// Unknown
		{name: "empty bucket", bucketName: "", expected: SourceUnknown},
		{name: "unknown bucket", bucketName: "in.c-something-random", expected: SourceUnknown},

		// Case insensitivity
		{name: "uppercase shopify", bucketName: "in.c-SHOPIFY", expected: SourceShopify},

		// False positive prevention - patterns should NOT match mid-string
		{name: "snowflake in compound name", bucketName: "in.c-my-snowflake-backup", expected: SourceUnknown},
		{name: "mysql in compound name", bucketName: "in.c-old-mysql-data", expected: SourceUnknown},
		{name: "s3 in compound name", bucketName: "in.c-archive-s3-files", expected: SourceUnknown},
		{name: "bigquery in compound name", bucketName: "in.c-legacy-bigquery-export", expected: SourceUnknown},

		// But prefix matches should still work
		{name: "shopify with suffix still matches", bucketName: "in.c-shopify-backup", expected: SourceShopify},
		{name: "hubspot with suffix still matches", bucketName: "in.c-hubspot-archive", expected: SourceHubspot},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := InferSourceFromBucket(tc.bucketName)
			assert.Equal(t, tc.expected, result, "InferSourceFromBucket(%q)", tc.bucketName)
		})
	}
}

func TestInferSourceFromTableID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		tableID  string
		expected string
	}{
		{name: "shopify table", tableID: "in.c-shopify.orders", expected: SourceShopify},
		{name: "output table", tableID: "out.c-processed.customers", expected: SourceTransformed},
		{name: "hubspot table", tableID: "in.c-hubspot.contacts", expected: SourceHubspot},
		{name: "unknown table", tableID: "in.c-custom.data", expected: SourceUnknown},
		{name: "empty table ID", tableID: "", expected: SourceUnknown},
		{name: "invalid table ID", tableID: "invalid", expected: SourceUnknown},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := InferSourceFromTableID(tc.tableID)
			assert.Equal(t, tc.expected, result, "InferSourceFromTableID(%q)", tc.tableID)
		})
	}
}

func TestGetSourceDisplayName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		source   string
		expected string
	}{
		{SourceShopify, "Shopify"},
		{SourceHubspot, "HubSpot"},
		{SourceSalesforce, "Salesforce"},
		{SourceS3, "Amazon S3"},
		{SourceTransformed, "Transformed"},
		{SourceUnknown, "Unknown"},
		{"invalid", "Unknown"},
	}

	for _, tc := range tests {
		t.Run(tc.source, func(t *testing.T) {
			t.Parallel()
			result := GetSourceDisplayName(tc.source)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestGetSourceType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		source   string
		expected string
	}{
		{SourceShopify, "e-commerce"},
		{SourceHubspot, "crm"},
		{SourceStripe, "payments"},
		{SourceGoogleAds, "advertising"},
		{SourceMySQL, "database"},
		{SourceSnowflake, "data-warehouse"},
		{SourceS3, "cloud-storage"},
		{SourceSlack, "communication"},
		{SourceJira, "project-management"},
		{SourceSegment, "analytics"},
		{SourceTransformed, "transformed"},
		{SourceManual, "manual"},
		{SourceUnknown, "unknown"},
	}

	for _, tc := range tests {
		t.Run(tc.source, func(t *testing.T) {
			t.Parallel()
			result := GetSourceType(tc.source)
			assert.Equal(t, tc.expected, result)
		})
	}
}
