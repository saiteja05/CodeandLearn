terraform {
  required_version = ">= 1.5"

  # TODO: Add a remote backend to avoid local terraform.tfstate which may
  # contain sensitive resource attributes. Example:
  #
  # backend "s3" {
  #   bucket  = "your-terraform-state-bucket"
  #   key     = "mongodb-bench/terraform.tfstate"
  #   region  = "us-east-1"
  #   encrypt = true
  # }

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

data "aws_ami" "al2023" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-*-x86_64"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

resource "aws_vpc" "bench" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = merge(var.tags, { Name = "mongodb-bench-vpc" })
}

resource "aws_internet_gateway" "bench" {
  vpc_id = aws_vpc.bench.id
  tags   = merge(var.tags, { Name = "mongodb-bench-igw" })
}

resource "aws_subnet" "bench" {
  count                   = 2
  vpc_id                  = aws_vpc.bench.id
  cidr_block              = cidrsubnet(aws_vpc.bench.cidr_block, 8, count.index)
  availability_zone       = data.aws_availability_zones.available.names[count.index]
  map_public_ip_on_launch = var.assign_public_ip

  tags = merge(var.tags, { Name = "mongodb-bench-subnet-${count.index}" })
}

data "aws_availability_zones" "available" {
  state = "available"
}

resource "aws_route_table" "bench" {
  vpc_id = aws_vpc.bench.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.bench.id
  }

  tags = merge(var.tags, { Name = "mongodb-bench-rt" })
}

resource "aws_route_table_association" "bench" {
  count          = 2
  subnet_id      = aws_subnet.bench[count.index].id
  route_table_id = aws_route_table.bench.id
}

resource "aws_security_group" "bench_client" {
  name_prefix = "mongodb-bench-client-"
  vpc_id      = aws_vpc.bench.id

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.allowed_ssh_cidr]
    description = "SSH access from allowed CIDR only"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
    description = "All outbound"
  }

  tags = merge(var.tags, { Name = "mongodb-bench-client-sg" })
}

resource "aws_iam_role" "bench_client" {
  name_prefix = "mongodb-bench-client-"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "ec2.amazonaws.com"
      }
    }]
  })

  tags = var.tags
}

resource "aws_iam_role_policy" "bench_s3_access" {
  name_prefix = "bench-s3-access-"
  role        = aws_iam_role.bench_client.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "s3:GetObject",
        "s3:PutObject",
        "s3:ListBucket"
      ]
      Resource = [
        "arn:aws:s3:::${var.results_bucket}",
        "arn:aws:s3:::${var.results_bucket}/*"
      ]
    }]
  })
}

resource "aws_iam_instance_profile" "bench_client" {
  name_prefix = "mongodb-bench-client-"
  role        = aws_iam_role.bench_client.name
}

resource "aws_instance" "bench_client" {
  count                = var.client_count
  ami                  = data.aws_ami.al2023.id
  instance_type        = var.instance_type
  key_name             = var.key_name
  subnet_id            = aws_subnet.bench[count.index % 2].id
  iam_instance_profile = aws_iam_instance_profile.bench_client.name

  disable_api_termination = var.termination_protection

  vpc_security_group_ids = [aws_security_group.bench_client.id]

  root_block_device {
    encrypted   = true
    volume_size = var.root_volume_size_gb
    volume_type = "gp3"
    iops        = 3000
    throughput  = 125
  }

  user_data = base64encode(templatefile("${path.module}/user_data.sh.tpl", {
    bench_binary_s3 = var.bench_binary_s3
    bench_config_s3 = var.bench_config_s3
    client_id       = count.index
    total_clients   = var.client_count
    results_bucket  = var.results_bucket
  }))

  tags = merge(var.tags, {
    Name     = "mongodb-bench-client-${count.index}"
    ClientID = count.index
  })
}
