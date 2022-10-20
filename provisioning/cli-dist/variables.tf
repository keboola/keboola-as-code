variable "keboola_stack" {
  type    = string
  default = "keboola-dev"
}

variable "bucket_name" {
  type = string
}

variable "github_oidc_provider_arn" {
  type = string
}

variable "aws_acm_certificate_arn" {
  type = string
}

variable "distribution_domain_name" {
  type = string
}

variable "distribution_domain_alias_name" {
  type = string
}