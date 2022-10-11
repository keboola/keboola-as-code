terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.34"
    }
  }
  required_version = ">= 0.14.9"
}

provider "aws" {
  region = "us-east-1"
  default_tags {
    tags = {
      KeboolaStack = var.keboola_stack
    }
  }
}
