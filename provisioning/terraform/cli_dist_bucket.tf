// https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/s3_bucket

resource "aws_s3_bucket" "cli_dist_bucket" {
  bucket = var.bucket_name
}

// https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/s3_bucket_acl
resource "aws_s3_bucket_acl" "cli_dist_bucket_acl" {
  bucket = aws_s3_bucket.cli_dist_bucket.id

  acl = "public-read"
}

// https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/s3_bucket_cors_configuration
resource "aws_s3_bucket_cors_configuration" "cli_dist_bucket_cors" {
  bucket = aws_s3_bucket.cli_dist_bucket.id

  cors_rule {
    allowed_methods = ["GET", "HEAD"]
    allowed_origins = ["*"]
    max_age_seconds = 86400
  }
}

// https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/cloudfront_origin_access_identity
data "aws_iam_policy_document" "cli_dist_bucket_access_policy_document" {
  statement {
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.cli_dist_bucket.arn}/*"]

    principals {
      type        = "AWS"
      identifiers = [aws_cloudfront_origin_access_identity.cli_dist_cloudfront_origin_access_identity.iam_arn]
    }
  }
}

resource "aws_s3_bucket_policy" "cli_dist_bucket_access_policy" {
  bucket = aws_s3_bucket.cli_dist_bucket.id

  policy = data.aws_iam_policy_document.cli_dist_bucket_access_policy_document.json
}

// https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/s3_bucket_website_configuration
resource "aws_s3_bucket_website_configuration" "cli_dist_bucket_website_configuration" {
  bucket = aws_s3_bucket.cli_dist_bucket.bucket

  index_document {
    suffix = "index.html"
  }
}

resource "aws_s3_object" "cli_dist_index" {
  bucket = aws_s3_bucket.cli_dist_bucket.id

  key          = "index.html"
  source       = "./index.html"
  acl          = "public-read"
  etag         = filemd5("./index.html")
  content_type = "text/html"
}
