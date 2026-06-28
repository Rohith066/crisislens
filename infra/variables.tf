variable "region" {
  description = "AWS region to deploy into"
  type        = string
  default     = "us-east-1"
}

variable "instance_type" {
  description = "EC2 instance type (t3.micro / t2.micro are free-tier eligible, region-dependent)"
  type        = string
  default     = "t3.micro"
}

variable "image" {
  description = "Container image to run (must be PUBLIC on GHCR so EC2 can pull without auth)"
  type        = string
  default     = "ghcr.io/rohith066/crisislens-api:latest"
}
