output "public_url" {
  description = "Open in a browser (allow ~2 min after apply for boot + image pull)"
  value       = "http://${aws_instance.crisislens.public_ip}/"
}

output "public_ip" {
  value = aws_instance.crisislens.public_ip
}
