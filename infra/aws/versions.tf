terraform {
  # 1.13 is the floor because the module uses no feature newer than it, and pinning
  # a floor lower than that would be a claim I have not tested. There is no upper
  # bound: this module is validated in CI against whatever Terraform is current,
  # which is the only way to find out early that it has stopped being valid.
  required_version = ">= 1.13"

  required_providers {
    aws = {
      source = "hashicorp/aws"
      # Pessimistic to the minor version. The AWS provider makes breaking changes
      # at major versions and adds resources at minor ones, so `~> 6.65` takes
      # patches automatically and nothing else.
      version = "~> 6.65"
    }
  }

  # No `backend` block, deliberately. State configuration would imply state, and
  # there is none — see the banner in README.md. What it would be, if this were
  # ever applied, is written down there in prose rather than left here commented
  # out for someone to uncomment by accident.
}

provider "aws" {
  region = var.region

  # Every resource this module would create is tagged with where it came from.
  # The first question asked about an unexplained resource in a shared account is
  # "who owns this", and a repository URL answers it without a wiki.
  default_tags {
    tags = {
      Project     = var.project
      Environment = var.environment
      ManagedBy   = "terraform"
      Repository  = "github.com/william-sarkar/wikistream-lakehouse"
    }
  }
}
