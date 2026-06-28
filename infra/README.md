# CrisisLens — AWS deploy (Terraform)

Provisions a free-tier EC2 instance that runs the serving image (Leaflet map +
`/hazards` feed) published to GHCR by CI, and outputs a public URL.

## Prerequisites
1. An AWS account.
2. AWS CLI configured: `aws configure` (IAM access key with EC2 permissions).
3. Terraform installed.
4. The GHCR image must be **public** so the instance can pull it without auth:
   GitHub → your profile → Packages → `crisislens-api` → Package settings → Change visibility → Public.

## Deploy
```bash
cd infra
terraform init
terraform apply        # review the plan, type 'yes'
```
`terraform apply` prints `public_url` — open it in ~2 minutes (first boot installs
Docker and pulls the image).

## Tear down (do this when you're done — keeps you at $0)
```bash
terraform destroy
```

## Notes
- Default region `us-east-1`, instance `t3.micro`. Override: `terraform apply -var region=us-west-2`.
- Only port 80 is open. No SSH key is provisioned (use EC2 Instance Connect / SSM if you need a shell).
- The heavy pipeline (Kafka, Spark, the RAG/LLM) is **not** deployed — it runs locally / as code. This serves the gold snapshot baked into the image.
