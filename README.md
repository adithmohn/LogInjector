# LogInjector
A microservice to inject logs from S3 to opensearch 

# AWS S3 Log Ingestor to OpenSearch 🚀

This service automatically ingests logs from an **S3 bucket** and sends them to an **OpenSearch** domain.  
It can be triggered manually via an API endpoint or continuously through **SQS events**.

---

## ⚙️ Deployment Options

You have a few options for deploying this service. Choose the one that best fits your environment.

---

### 🟢 Option A: Run Locally / on EC2

This method is great for quick testing or for a simple EC2-based deployment.

1. Export environment variables:

```bash
export AWS_REGION=eu-west-1
export OPENSEARCH_HOST=vpc-your-opensearch-domain.eu-west-1.es.amazonaws.com
export OPENSEARCH_INDEX=aws-s3-log
export SQS_QUEUE_URL=https://sqs.eu-west-1.amazonaws.com/123456789012/alb-log-opensearch

    Start the service:

python app.py

    Check health:

curl http://localhost:5000/health

🐳 Option B: Run with Docker

For a containerized approach, you can use Docker.

    Build the Docker image:

docker build -t alb-ingestor .

    Run the container:

docker run -p 5000:5000 \
  -e AWS_REGION=eu-west-1 \
  -e OPENSEARCH_HOST=... \
  -e OPENSEARCH_INDEX=aws-s3-log \
  -e SQS_QUEUE_URL=... \
  alb-ingestor

☸️ Option C: Kubernetes (IRSA Ready)

This option uses Kubernetes with IAM Roles for Service Accounts (IRSA) for a secure, scalable, and manageable deployment.

    Create an IAM Role with S3 and SQS permissions.

    Create a ServiceAccount in Kubernetes and annotate it with the IAM Role.

    Apply the deployment manifest:

apiVersion: apps/v1
kind: Deployment
metadata:
  name: s3-opensearch-ingestor
  namespace: opensearch
spec:
  replicas: 1
  selector:
    matchLabels:
      app: s3-opensearch-ingestor
  template:
    metadata:
      labels:
        app: s3-opensearch-ingestor
    spec:
      serviceAccountName: uattest-s3-sa
      containers:
        - name: ingestor
          image: <your ECR repo>/alb-ingestor:latest
          env:
            - name: AWS_REGION
              value: "eu-west-1"
            - name: OPENSEARCH_HOST
              value: "vpc-your-os-domain.es.amazonaws.com"
            - name: OPENSEARCH_INDEX
              value: "aws-s3-log"
            - name: SQS_QUEUE_URL
              value: "https://sqs.eu-west-1.amazonaws.com/123456789012/alb-log-opensearch"

🔒 IAM Permissions

The service requires an IAM role with the necessary permissions to interact with S3, SQS, and OpenSearch.

    For local or EC2 deployments, attach this role to the EC2 instance profile.

    For Kubernetes, use IRSA to associate the role with the service account.

Here is a sample IAM policy that grants the required permissions:

{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject"
      ],
      "Resource": "arn:aws:s3:::<your-s3-bucket-name>/*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "sqs:ReceiveMessage",
        "sqs:DeleteMessage",
        "sqs:GetQueueAttributes"
      ],
      "Resource": "arn:aws:sqs:<your-region>:<your-account-id>:<your-sqs-queue-name>"
    },
    {
      "Effect": "Allow",
      "Action": [
        "es:ESHttp*",
        "es:DescribeDomain"
      ],
      "Resource": "arn:aws:es:<your-region>:<your-account-id>:domain/<your-opensearch-domain-name>/*"
    },
    {
      "Effect": "Allow",
      "Action": "sts:GetCallerIdentity",
      "Resource": "*"
    }
  ]
}

⚠️ Note: Replace the placeholder values (<your-s3-bucket-name>, <your-region>, etc.) with your actual AWS resource information.
This policy follows the principle of least privilege.
🧪 Endpoints

The service provides the following endpoints:

    GET /health → A simple health check.

    POST /ingest?key=... → Manually ingest a specific object from S3.

    Auto-ingestion → Automatically processes new S3 objects from the configured SQS queue.

📎 GitHub Repository

👉 [Insert GitHub repo link here]
📝 Roadmap

We are continuously working to improve this service. Planned features:

Single S3 file ingestion

Auto ingestion via SQS

Kubernetes deployment with IRSA

Helm chart for easy installation

Prometheus metrics for monitoring

Batch re-processing mode
