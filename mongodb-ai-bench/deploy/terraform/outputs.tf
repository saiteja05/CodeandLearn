output "client_public_ips" {
  description = "Public IPs of benchmark client instances"
  value       = aws_instance.bench_client[*].public_ip
}

output "client_private_ips" {
  description = "Private IPs of benchmark client instances"
  value       = aws_instance.bench_client[*].private_ip
}

output "vpc_id" {
  description = "VPC ID for Atlas peering"
  value       = aws_vpc.bench.id
}

output "vpc_cidr" {
  description = "VPC CIDR block"
  value       = aws_vpc.bench.cidr_block
}

output "ssh_commands" {
  description = "SSH commands for each client"
  value = [
    for i, ip in aws_instance.bench_client[*].public_ip :
    "ssh -i ~/.ssh/${var.key_name}.pem ec2-user@${ip}"
  ]
}
