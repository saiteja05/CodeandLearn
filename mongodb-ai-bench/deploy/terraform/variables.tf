variable "aws_region" {
  description = "AWS region for benchmark clients"
  type        = string
  default     = "us-east-1"
}

variable "instance_type" {
  description = "EC2 instance type for benchmark clients"
  type        = string
  default     = "c6i.4xlarge"
}

variable "client_count" {
  description = "Number of EC2 benchmark client instances"
  type        = number
  default     = 4
}

variable "key_name" {
  description = "SSH key pair name for EC2 access"
  type        = string
}

variable "atlas_vpc_cidr" {
  description = "CIDR block of the Atlas VPC for peering"
  type        = string
  default     = "192.168.248.0/21"
}

variable "atlas_project_id" {
  description = "MongoDB Atlas project ID for VPC peering"
  type        = string
  default     = ""
}

variable "bench_binary_s3" {
  description = "S3 URI of the pre-built benchmark binary (e.g. s3://my-bucket/mongodb-ai-bench-linux-amd64)"
  type        = string
}

variable "bench_config_s3" {
  description = "S3 URI of the benchmark config YAML"
  type        = string
}

variable "tags" {
  description = "Tags to apply to all resources"
  type        = map(string)
  default = {
    Project     = "mongodb-ai-bench"
    Environment = "benchmark"
  }
}
