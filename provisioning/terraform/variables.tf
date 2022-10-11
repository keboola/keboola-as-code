variable "keboola_stack" {
  type    = string
  default = "keboola-dev"
}

variable "cli_dist_bucket_name" {
  type = string
}

variable "github_oidc_provider_arn" {
  type = string
}
