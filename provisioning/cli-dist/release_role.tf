# IAM role which allows production release workflow push artifacts to cli-dist S3 bucket

data "aws_iam_policy_document" "cli_dist_release_assume_policy_doc" {
  version = "2012-10-17"
  statement {
    sid = "AllowAssumeRoleFromGithub"
    actions = [
      "sts:AssumeRoleWithWebIdentity"
    ]
    effect = "Allow"
    principals {
      identifiers = [
        var.github_oidc_provider_arn
      ]
      type = "Federated"
    }
    condition {
      test     = "StringLike"
      variable = "token.actions.githubusercontent.com:sub"
      values = [
        "repo:keboola/keboola-as-code:ref:refs/tags/*",
      ]
    }
    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values = [
        "sts.amazonaws.com"
      ]
    }
  }
}

data "aws_iam_policy_document" "cli_dist_release_resources_policy_doc" {
  version = "2012-10-17"
  statement {
    effect = "Allow"
    actions = [
      "s3:*"
    ]
    resources = [
      "${aws_s3_bucket.cli_dist_bucket.arn}/*"
    ]
  }
  statement {
    effect = "Allow"
    actions = [
      "s3:ListBucket"
    ]
    resources = [
      aws_s3_bucket.cli_dist_bucket.arn
    ]
  }
}

resource "aws_iam_role_policy" "cli_dist_release_resources_policy" {
  name_prefix   = "cli-dist-release-bucket-full-access"
  role   = aws_iam_role.cli_dist_release_role.id
  policy = data.aws_iam_policy_document.cli_dist_release_resources_policy_doc.json
}

resource "aws_iam_role" "cli_dist_release_role" {
  name               = "cli-dist-release"
  assume_role_policy = data.aws_iam_policy_document.cli_dist_release_assume_policy_doc.json
}