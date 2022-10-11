// https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/cloudfront_distribution

locals {
  cli_dist_default_origin_id = "cli_dist_default_origin"
}

// 88a5eaf4-2fd4-4709-b370-b4c650ea3fcf
data "aws_cloudfront_origin_request_policy" "aws_managed_origin_request_policy_cors_s3_origin" {
  name = "Managed-CORS-S3Origin"
}

resource "aws_cloudfront_distribution" "cli_dist_cloudfront" {
  depends_on = [
    aws_s3_bucket.cli_dist_bucket
  ]
  aliases         = [
    var.distribution_domain_name
  ]
  enabled         = true
  http_version    = "http2"
  is_ipv6_enabled = true
  default_cache_behavior {
    cache_policy_id          = aws_cloudfront_cache_policy.cli_dist_cloudfront_cache_policy.id
    compress                 = false
    origin_request_policy_id = data.aws_cloudfront_origin_request_policy.aws_managed_origin_request_policy_cors_s3_origin.id
    cached_methods           = ["GET", "HEAD"]
    target_origin_id         = local.cli_dist_default_origin_id
    viewer_protocol_policy   = "https-only"
    allowed_methods          = ["GET", "HEAD"]
  }
  origin {
    domain_name = aws_s3_bucket.cli_dist_bucket.bucket_regional_domain_name
    origin_id   = local.cli_dist_default_origin_id
    s3_origin_config {
      origin_access_identity = aws_cloudfront_origin_access_identity.cli_dist_cloudfront_origin_access_identity.cloudfront_access_identity_path
    }
  }
  viewer_certificate {
    acm_certificate_arn      = var.aws_acm_certificate_arn
    minimum_protocol_version = "TLSv1.2_2021"
    ssl_support_method       = "sni-only"
  }
  restrictions {
    geo_restriction {
      restriction_type = "none"
    }
  }
}

// https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/cloudfront_cache_policy
resource "aws_cloudfront_cache_policy" "cli_dist_cloudfront_cache_policy" {
  name    = "CachedCorsRelatedHeaders-CompressionSupport"
  comment = "Cache Origin, Access-Control-Request-Method/Headers; Support gzip/brotli compressions"
  min_ttl = 1
  parameters_in_cache_key_and_forwarded_to_origin {
    headers_config {
      header_behavior = "whitelist"
      headers {
        items = ["Origin", "Access-Control-Request-Method", "Access-Control-Request-Headers"]
      }
    }
    cookies_config {
      cookie_behavior = "none"
    }
    query_strings_config {
      query_string_behavior = "none"
    }
    enable_accept_encoding_gzip   = true
    enable_accept_encoding_brotli = true
  }
}

// https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/cloudfront_origin_access_identity
resource "aws_cloudfront_origin_access_identity" "cli_dist_cloudfront_origin_access_identity" {
  comment = "Origin Access Identity for accessing s3 buckets"
}
