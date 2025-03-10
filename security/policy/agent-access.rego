package agent.access

import future.keywords
import input

#==============#
# Base Policy  #
#==============#

default allow := false
default deny := false
default audit := false
default reason := ""

#=====================#
# Core Access Control #
#=====================#

# Access decision combines authentication and authorization checks
allow {
    authenticated_agent
    authorized_operation
    validate_environment
    validate_timing
    validate_rate_limit
}

deny {
    not allow
    reason := "Access denied by policy"
}

#=======================#
# Authentication Rules  #
#=======================#

# Multi-factor authentication requirements
authenticated_agent {
    valid_api_key
    valid_certificate
    valid_otp
}

valid_api_key {
    input.authentication.api_key == data.agents[input.agent.id].api_key
}

valid_certificate {
    input.authentication.client_cert.subject.commonName == input.agent.id
    data.cert_authorities[input.authentication.client_cert.issuer]
}

valid_otp {
    time.now_ns() - input.authentication.otp_timestamp < 300000000000  # 5 minutes
    crypto.x509.parse_and_verify_rsa(input.authentication.otp_signature, input.agent.id)
}

#=======================#
# Authorization Rules   #
#=======================#

authorized_operation {
    has_required_role
    has_required_permission
    within_scope
    not restricted_operation
}

has_required_role {
    data.roles[input.agent.role].permissions[_] == input.request.operation
}

has_required_permission {
    input.request.operation in data.permissions[input.agent.id]
}

within_scope {
    input.request.scope == data.agents[input.agent.id].allowed_scopes[_]
}

restricted_operation {
    data.restricted_operations[_] == input.request.operation
    not data.agents[input.agent.id].privileged
}

#=======================#
# Environmental Checks  #
#=======================#

validate_environment {
    allowed_geo_location
    secure_connection
    approved_device
}

allowed_geo_location {
    input.environment.ip_address == data.allowed_ips[_]
    input.environment.country_code == data.allowed_countries[_]
}

secure_connection {
    input.environment.protocol == "TLSv1.3"
    input.environment.cipher_suite == "AEAD-AES256-GCM-SHA384"
}

approved_device {
    input.agent.device_fingerprint in data.trusted_devices
    input.agent.device_encrypted == true
}

#=====================#
# Temporal Constraints #
#=====================#

validate_timing {
    within_operational_window
    check_expiration
    validate_token_freshness
}

within_operational_window {
    time.clock(input.environment.timestamp)[0] >= 8
    time.clock(input.environment.timestamp)[0] < 20
}

check_expiration {
    time.now_ns() < data.agents[input.agent.id].credentials_expire_at
}

validate_token_freshness {
    time.now_ns() - input.authentication.session_start < data.session_timeouts[input.agent.tier]
}

#=====================#
# Rate Limiting       #
#=====================#

validate_rate_limit {
    check_global_rate
    check_local_rate
}

check_global_rate {
    count(agent_requests[input.agent.id]) < data.rate_limits[input.agent.tier].global
}

check_local_rate {
    count(agent_requests[input.agent.id][input.request.operation]) < data.rate_limits[input.agent.tier].per_operation
}

#=====================#
# Audit Logging       #
#=====================#

audit {
    log_decision
    log_sensitive_operations
    capture_metadata
}

log_decision {
    audit := true
    reason := sprintf("Decision: %v for %v", [allow, input.agent.id])
}

log_sensitive_operations {
    input.request.operation == data.sensitive_operations[_]
    reason := sprintf("%s; Sensitive operation: %v", [reason, input.request.operation])
}

capture_metadata {
    reason := sprintf("%s | IP: %v | Device: %v", [reason, input.environment.ip_address, input.agent.device_id])
}

#=====================#
# Utility Functions   #
#=====================#

agent_requests[agent_id] = request {
    request := data.requests[_]
    request.agent == agent_id
}

#=====================#
# Data Structures     #
#=====================#

# Sample Data Model (would be loaded externally)
agents = {
    "agent-001": {
        "role": "data-processor",
        "allowed_scopes": ["dataset-a", "dataset-b"],
        "privileged": false,
        "tier": "gold",
        "api_key": "secure-key-123",
        "credentials_expire_at": 1735689600000000000  # 2025-01-01
    }
}

roles = {
    "data-processor": {
        "permissions": ["read_dataset", "transform_data"]
    }
}

rate_limits = {
    "gold": {
        "global": 1000,
        "per_operation": 100
    }
}

allowed_countries = ["US", "CA", "GB"]
allowed_ips = ["192.168.1.0/24", "10.0.0.0/8"]
