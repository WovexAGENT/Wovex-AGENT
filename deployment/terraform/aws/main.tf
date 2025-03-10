terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.23"
    }
    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.11"
    }
  }
}

provider "aws" {
  region = var.aws_region
  default_tags {
    tags = {
      Environment   = var.environment
      Terraform     = "true"
      Project       = "production-cluster"
      Owner         = "devops-team"
      CostCenter    = "12345"
      DataClass     = "restricted"
      Compliance    = "hipaa"
      BackupEnabled = "true"
    }
  }
}

locals {
  cluster_name = "${var.environment}-eks-cluster"
  azs          = slice(data.aws_availability_zones.available.names, 0, 3)
}

data "aws_availability_zones" "available" {
  state = "available"
}

data "aws_caller_identity" "current" {}

module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "5.0.0"

  name = "${local.cluster_name}-vpc"
  cidr = var.vpc_cidr

  azs             = local.azs
  private_subnets = [for k, v in local.azs : cidrsubnet(var.vpc_cidr, 4, k)]
  public_subnets  = [for k, v in local.azs : cidrsubnet(var.vpc_cidr, 8, k + 48)]
  intra_subnets   = [for k, v in local.azs : cidrsubnet(var.vpc_cidr, 8, k + 52)]

  enable_nat_gateway     = true
  single_nat_gateway     = false
  one_nat_gateway_per_az = true

  enable_dns_hostnames = true
  enable_dns_support   = true

  public_subnet_tags = {
    "kubernetes.io/role/elb" = "1"
  }

  private_subnet_tags = {
    "kubernetes.io/role/internal-elb" = "1"
  }

  tags = {
    Tier        = "network"
    NetworkType = "multi-az"
  }
}

module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "19.15.3"

  cluster_name                   = local.cluster_name
  cluster_version                = "1.27"
  cluster_endpoint_public_access = true

  cluster_addons = {
    coredns = {
      most_recent = true
    }
    kube-proxy = {
      most_recent = true
    }
    vpc-cni = {
      most_recent = true
    }
    aws-ebs-csi-driver = {
      most_recent = true
    }
  }

  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.private_subnets

  eks_managed_node_group_defaults = {
    ami_type       = "AL2_x86_64"
    instance_types = ["m5.large", "m5a.large", "m5d.large"]
    disk_size      = 50
    disk_type      = "gp3"
    iam_role_additional_policies = {
      AmazonEBSCSIDriverPolicy = "arn:aws:iam::aws:policy/service-role/AmazonEBSCSIDriverPolicy"
    }
  }

  eks_managed_node_groups = {
    main = {
      name           = "managed-node-group"
      min_size       = 3
      max_size       = 10
      desired_size   = 3
      instance_types = ["m5.large"]
      capacity_type  = "SPOT"

      labels = {
        NodeGroupType = "primary"
      }

      taints = {
        dedicated = {
          key    = "dedicated"
          value  = "true"
          effect = "NO_SCHEDULE"
        }
      }

      update_config = {
        max_unavailable_percentage = 33
      }

      tags = {
        AutoScalingGroup = "true"
        ScalingPolicy    = "cost-optimized"
      }
    }

    gpu = {
      name           = "gpu-node-group"
      min_size       = 1
      max_size       = 3
      desired_size   = 1
      instance_types = ["p3.2xlarge"]
      ami_type       = "AL2_x86_64_GPU"
      capacity_type  = "ON_DEMAND"

      labels = {
        Accelerator = "nvidia"
      }

      taints = [{
        key    = "nvidia.com/gpu"
        value  = "true"
        effect = "NO_SCHEDULE"
      }]
    }
  }

  node_security_group_additional_rules = {
    ingress_cluster_ports = {
      description = "Cluster API server to node group"
      protocol    = "tcp"
      from_port   = 1025
      to_port     = 65535
      type        = "ingress"
      cidr_blocks = [module.vpc.vpc_cidr_block]
    }
  }

  tags = {
    KubernetesCluster = "true"
    AutoHealing       = "enabled"
  }
}

module "rds" {
  source  = "terraform-aws-modules/rds/aws"
  version = "6.1.1"

  identifier = "${local.cluster_name}-rds"

  engine               = "postgres"
  engine_version       = "15.3"
  instance_class       = "db.m5d.large"
  allocated_storage    = 100
  storage_type         = "gp3"
  storage_encrypted   = true
  kms_key_id          = aws_kms_key.rds.arn
  maintenance_window   = "Sat:03:00-Sat:05:00"
  backup_window        = "02:00-04:00"
  backup_retention_period = 14
  skip_final_snapshot  = false
  deletion_protection = true

  username = var.db_username
  password = var.db_password
  port     = 5432

  multi_az               = true
  db_subnet_group_name   = module.vpc.database_subnet_group_name
  vpc_security_group_ids = [module.rds_security_group.security_group_id]

  parameters = [
    {
      name  = "autovacuum"
      value = "1"
    },
    {
      name  = "client_encoding"
      value = "utf8"
    }
  ]

  tags = {
    DataTier    = "database"
    Replication = "multi-az"
  }
}

resource "aws_kms_key" "rds" {
  description             = "KMS key for RDS encryption"
  deletion_window_in_days = 30
  enable_key_rotation     = true
  policy                  = data.aws_iam_policy_document.kms_policy.json
}

data "aws_iam_policy_document" "kms_policy" {
  statement {
    sid       = "Enable IAM User Permissions"
    actions   = ["kms:*"]
    resources = ["*"]
    principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"]
    }
  }
}

module "rds_security_group" {
  source  = "terraform-aws-modules/security-group/aws"
  version = "5.1.0"

  name        = "${local.cluster_name}-rds-sg"
  description = "Security group for RDS database"
  vpc_id      = module.vpc.vpc_id

  ingress_with_cidr_blocks = [
    {
      from_port   = 5432
      to_port     = 5432
      protocol    = "tcp"
      description = "PostgreSQL access from EKS"
      cidr_blocks = module.vpc.vpc_cidr_block
    }
  ]

  egress_rules = ["all-all"]
}

resource "aws_s3_bucket" "logging" {
  bucket = "${local.cluster_name}-logging-${data.aws_caller_identity.current.account_id}"
  force_destroy = false

  tags = {
    LoggingBucket = "true"
  }
}

resource "aws_s3_bucket_versioning" "logging" {
  bucket = aws_s3_bucket.logging.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "logging" {
  bucket = aws_s3_bucket.logging.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_cloudwatch_log_group" "eks" {
  name              = "/aws/eks/${local.cluster_name}/cluster"
  retention_in_days = 365
  kms_key_id        = aws_kms_key.cloudwatch.arn
}

resource "aws_kms_key" "cloudwatch" {
  description             = "KMS key for CloudWatch logs encryption"
  deletion_window_in_days = 7
  enable_key_rotation     = true
}

module "load_balancer_controller" {
  source  = "terraform-aws-modules/eks/aws//modules/load-balancer-controller"
  version = "19.15.3"

  cluster_name     = local.cluster_name
  cluster_identity_oidc_issuer = module.eks.cluster_oidc_issuer_url
  create_role      = true
  namespace        = "kube-system"
  service_account  = "aws-load-balancer-controller"
}

resource "aws_iam_policy" "external_dns" {
  name        = "ExternalDNS"
  description = "Policy for External DNS permissions"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "route53:ChangeResourceRecordSets"
        ]
        Resource = [
          "arn:aws:route53:::hostedzone/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "route53:ListHostedZones",
          "route53:ListResourceRecordSets"
        ]
        Resource = [
          "*"
        ]
      }
    ]
  })
}

module "external_dns" {
  source  = "terraform-aws-modules/eks/aws//modules/iam-role-for-service-accounts-irs"
  version = "19.15.3"

  role_name             = "external-dns"
  attach_external_dns_policy = true
  external_dns_hosted_zone_arns = ["arn:aws:route53:::hostedzone/XYZ123"]

  oidc_providers = {
    main = {
      provider_arn               = module.eks.oidc_provider_arn
      namespace_service_accounts = ["kube-system:external-dns"]
    }
  }
}

output "cluster_name" {
  value = module.eks.cluster_name
}

output "cluster_endpoint" {
  value = module.eks.cluster_endpoint
}

output "vpc_id" {
  value = module.vpc.vpc_id
}

output "rds_endpoint" {
  value = module.rds.db_instance_address
}
