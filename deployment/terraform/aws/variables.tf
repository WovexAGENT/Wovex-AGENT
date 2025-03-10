# Core Configuration
variable "aws_region" {
  description = "AWS region to deploy resources"
  type        = string
  default     = "us-west-2"
  validation {
    condition     = can(regex("^(us|eu|ap|sa|ca)-\\w+-\\d+$", var.aws_region))
    error_message = "Invalid AWS region format."
  }
}

variable "environment" {
  description = "Deployment environment (e.g., dev, staging, production)"
  type        = string
  default     = "production"
  validation {
    condition     = contains(["dev", "staging", "production"], var.environment)
    error_message = "Valid values: dev, staging, production."
  }
}

# Network Configuration
variable "vpc_cidr" {
  description = "CIDR block for the VPC"
  type        = string
  default     = "10.0.0.0/16"
  validation {
    condition     = can(cidrnetmask(var.vpc_cidr)) && cidrsubnet(var.vpc_cidr, 0, 0) == var.vpc_cidr
    error_message = "Must be a valid IPv4 CIDR block."
  }
}

variable "availability_zones" {
  description = "List of availability zones to use"
  type        = list(string)
  default     = ["us-west-2a", "us-west-2b", "us-west-2c"]
}

# EKS Cluster Configuration
variable "eks_cluster_version" {
  description = "Kubernetes cluster version"
  type        = string
  default     = "1.27"
  validation {
    condition     = can(regex("^1\\.(2[4-7]|3[0-9])$", var.eks_cluster_version))
    error_message = "Invalid EKS version. Must be 1.24-1.39."
  }
}

variable "eks_managed_node_groups" {
  description = "Configuration for EKS managed node groups"
  type = map(object({
    instance_types  = list(string)
    min_size        = number
    max_size        = number
    desired_size    = number
    capacity_type   = string
    disk_size       = number
    labels          = map(string)
    taints          = list(map(string))
  }))
  default = {
    main = {
      instance_types  = ["m5.large"]
      min_size        = 3
      max_size        = 10
      desired_size    = 3
      capacity_type   = "SPOT"
      disk_size       = 50
      labels          = { NodeGroupType = "primary" }
      taints          = []
    }
  }
}

# Database Configuration
variable "db_engine_version" {
  description = "RDS PostgreSQL engine version"
  type        = string
  default     = "15.3"
  validation {
    condition     = can(regex("^\\d+\\.\\d+$", var.db_engine_version))
    error_message = "Must be valid PostgreSQL version format."
  }
}

variable "db_instance_class" {
  description = "RDS instance type"
  type        = string
  default     = "db.m5d.large"
  validation {
    condition     = can(regex("^db\\.[a-z0-9]+\\.\\w+$", var.db_instance_class))
    error_message = "Invalid RDS instance type format."
  }
}

variable "db_username" {
  description = "Master username for RDS database"
  type        = string
  sensitive   = true
  default     = "admin"
}

variable "db_password" {
  description = "Master password for RDS database"
  type        = string
  sensitive   = true
  validation {
    condition     = length(var.db_password) >= 12 && can(regex("[A-Z]", var.db_password)) && can(regex("[a-z]", var.db_password)) && can(regex("[0-9]", var.db_password))
    error_message = "Password must be at least 12 characters with uppercase, lowercase, and numbers."
  }
}

# Security Configuration
variable "allowed_ingress_cidrs" {
  description = "List of allowed CIDR blocks for external access"
  type        = list(string)
  default     = ["10.0.0.0/16"]
}

variable "enable_encryption" {
  description = "Enable encryption for all storage resources"
  type        = bool
  default     = true
}

# Tagging Configuration
variable "standard_tags" {
  description = "Common tags to apply to all resources"
  type        = map(string)
  default = {
    Project       = "infrastructure-core"
    ManagedBy     = "Terraform"
    Compliance    = "HIPAA"
    DataClass     = "restricted"
    BackupEnabled = "true"
  }
}

# Storage Configuration
variable "ebs_volume_types" {
  description = "Configuration for EBS volume types"
  type = object({
    root_volume_type = string
    data_volume_type = string
  })
  default = {
    root_volume_type = "gp3"
    data_volume_type = "io2"
  }
  validation {
    condition     = contains(["gp3", "io2", "st1"], var.ebs_volume_types.root_volume_type) && contains(["gp3", "io2", "st1"], var.ebs_volume_types.data_volume_type)
    error_message = "Valid EBS types: gp3, io2, st1."
  }
}

# Monitoring Configuration
variable "cloudwatch_retention" {
  description = "CloudWatch log retention period in days"
  type        = number
  default     = 365
  validation {
    condition     = var.cloudwatch_retention >= 7 && var.cloudwatch_retention <= 3653
    error_message = "Retention period must be between 7 and 3653 days."
  }
}

variable "enable_enhanced_monitoring" {
  description = "Enable enhanced monitoring for RDS instances"
  type        = bool
  default     = true
}

# Auto Scaling Configuration
variable "asg_cpu_utilization_high" {
  description = "CPU utilization threshold for scaling up"
  type        = number
  default     = 70
  validation {
    condition     = var.asg_cpu_utilization_high >= 50 && var.asg_cpu_utilization_high <= 90
    error_message = "CPU threshold must be between 50-90%."
  }
}

variable "asg_memory_utilization_high" {
  description = "Memory utilization threshold for scaling up"
  type        = number
  default     = 65
  validation {
    condition     = var.asg_memory_utilization_high >= 50 && var.asg_memory_utilization_high <= 85
    error_message = "Memory threshold must be between 50-85%."
  }
}

# Backup Configuration
variable "backup_retention_period" {
  description = "Number of days to retain automated backups"
  type        = number
  default     = 14
  validation {
    condition     = var.backup_retention_period >= 1 && var.backup_retention_period <= 35
    error_message = "Backup retention must be between 1-35 days."
  }
}

variable "preferred_maintenance_window" {
  description = "Maintenance window for RDS instances"
  type        = string
  default     = "sat:03:00-sat:05:00"
  validation {
    condition     = can(regex("^[a-z]{3}:[0-9]{2}:[0-9]{2}-[a-z]{3}:[0-9]{2}:[0-9]{2}$", var.preferred_maintenance_window))
    error_message = "Must be in 'ddd:hh24:mi-ddd:hh24:mi' format."
  }
}

# GPU Configuration
variable "enable_gpu_nodes" {
  description = "Deploy GPU-optimized worker nodes"
  type        = bool
  default     = false
}

variable "gpu_instance_types" {
  description = "List of GPU instance types"
  type        = list(string)
  default     = ["p3.2xlarge", "g4dn.xlarge"]
  validation {
    condition     = alltrue([for t in var.gpu_instance_types : can(regex("^(p|g)\\d", t))])
    error_message = "Must be valid GPU instance types."
  }
}

# Cost Management
variable "budget_limit" {
  description = "Monthly budget limit in USD"
  type        = number
  default     = 5000
  validation {
    condition     = var.budget_limit >= 100 && var.budget_limit <= 100000
    error_message = "Budget must be between 100-100,000 USD."
  }
}

variable "reserved_instance_coverage" {
  description = "Percentage of instances to cover with RIs"
  type        = number
  default     = 60
  validation {
    condition     = var.reserved_instance_coverage >= 0 && var.reserved_instance_coverage <= 100
    error_message = "Coverage must be between 0-100%."
  }
}

# Advanced Networking
variable "enable_transit_gateway" {
  description = "Enable Transit Gateway for multi-VPC networking"
  type        = bool
  default     = false
}

variable "enable_vpc_flow_logs" {
  description = "Enable VPC Flow Logs to S3"
  type        = bool
  default     = true
}
