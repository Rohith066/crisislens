terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.region
}

# Latest Amazon Linux 2023 AMI for this region (no hard-coded AMI id).
data "aws_ssm_parameter" "al2023" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
}

# Security group: allow inbound HTTP (80) from anywhere; all outbound.
resource "aws_security_group" "crisislens" {
  name        = "crisislens-sg"
  description = "CrisisLens serving API (HTTP 80)"

  ingress {
    description = "HTTP"
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Project = "crisislens" }
}

# Cloud-init: install Docker on boot and run the published image on port 80.
locals {
  user_data = <<-EOF
    #!/bin/bash
    dnf install -y docker
    systemctl enable --now docker
    docker run -d --restart always -p 80:8000 ${var.image}
  EOF
}

resource "aws_instance" "crisislens" {
  ami                    = nonsensitive(data.aws_ssm_parameter.al2023.value)
  instance_type          = var.instance_type
  vpc_security_group_ids = [aws_security_group.crisislens.id]
  user_data              = local.user_data

  tags = { Name = "crisislens-api", Project = "crisislens" }
}
