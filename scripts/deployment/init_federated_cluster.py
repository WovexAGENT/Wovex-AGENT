#!/usr/bin/env python3
import argparse
import os
import sys
import json
import random
import subprocess
import shutil
import socket
from pathlib import Path
from typing import Dict, List, Tuple
from cryptography import x509
from cryptography.x509.oid import NameOID
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.backends import default_backend

DEFAULT_PORTS = (8000, 9000)
CERTS_DIR = Path(__file__).parent / "certs"
CONFIG_DIR = Path(__file__).parent / "configs"
TEMPLATES_DIR = Path(__file__).parent / "templates"

def validate_environment() -> None:
    required_binaries = ["docker", "openssl", "kubectl"]
    missing = []
    for binary in required_binaries:
        if not shutil.which(binary):
            missing.append(binary)
    if missing:
        raise SystemExit(f"Missing required binaries: {', '.join(missing)}")

def generate_rsa_keypair(key_path: Path) -> None:
    key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
        backend=default_backend()
    )
    with open(key_path, "wb") as f:
        f.write(key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption()
        ))

def generate_self_signed_cert(key_path: Path, cert_path: Path, hostname: str) -> None:
    with open(key_path, "rb") as f:
        key = serialization.load_pem_private_key(f.read(), password=None, backend=default_backend())

    subject = issuer = x509.Name([
        x509.NameAttribute(NameOID.COUNTRY_NAME, "US"),
        x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, "CA"),
        x509.NameAttribute(NameOID.LOCALITY_NAME, "San Francisco"),
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, "Federated Learning Inc"),
        x509.NameAttribute(NameOID.COMMON_NAME, hostname),
    ])

    cert = x509.CertificateBuilder().subject_name(
        subject
    ).issuer_name(
        issuer
    ).public_key(
        key.public_key()
    ).serial_number(
        x509.random_serial_number()
    ).not_valid_before(
        datetime.datetime.utcnow()
    ).not_valid_after(
        datetime.datetime.utcnow() + datetime.timedelta(days=365)
    ).add_extension(
        x509.SubjectAlternativeName([x509.DNSName(hostname)]),
        critical=False,
    ).sign(key, hashes.SHA256(), default_backend())

    with open(cert_path, "wb") as f:
        f.write(cert.public_bytes(serialization.Encoding.PEM))

def find_available_port(start: int, end: int) -> int:
    for port in range(start, end + 1):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            if s.connect_ex(('localhost', port)) != 0:
                return port
    raise SystemExit("No available ports in specified range")

def generate_node_config(cluster_name: str, node_type: str, port: int, index: int) -> Dict:
    return {
        "cluster": {
            "name": cluster_name,
            "discovery_mode": "dynamic",
            "auth": {
                "tls": {
                    "enabled": True,
                    "cert_path": str(CERTS_DIR / f"{node_type}-{index}.crt"),
                    "key_path": str(CERTS_DIR / f"{node_type}-{index}.key")
                }
            }
        },
        "network": {
            "host": "0.0.0.0",
            "port": port,
            "max_message_size": "10MB",
            "compression": "zlib"
        },
        "logging": {
            "level": "INFO",
            "format": "json",
            "rotation": "100MB"
        },
        "resources": {
            "max_memory": "4GB",
            "cpu_cores": 2,
            "gpu_allocation": "shared"
        }
    }

def generate_coordinator_config(cluster_name: str, port: int) -> Dict:
    config = generate_node_config(cluster_name, "coordinator", port, 0)
    config["coordinator"] = {
        "federation_strategy": "fedavg",
        "model_registry": {
            "storage": "s3",
            "bucket": f"{cluster_name}-models",
            "checkpoint_interval": "30m"
        },
        "participant_selection": {
            "strategy": "random",
            "minimum_nodes": 3,
            "quality_threshold": 0.85
        }
    }
    return config

def generate_worker_config(cluster_name: str, port: int, index: int) -> Dict:
    config = generate_node_config(cluster_name, "worker", port, index)
    config["worker"] = {
        "data": {
            "path": "/var/feddata",
            "privacy": {
                "technique": "differential_privacy",
                "epsilon": 3.0,
                "delta": 1e-5
            }
        },
        "compute": {
            "batch_size": 32,
            "max_epochs": 10,
            "validation_split": 0.2
        }
    }
    return config

def generate_docker_compose(cluster_name: str, nodes: List[Tuple[str, int]]) -> None:
    compose = {
        "version": "3.8",
        "services": {},
        "volumes": {
            "shared_models": {"driver": "local"},
            "training_data": {"driver": "local"}
        },
        "networks": {
            "fednet": {
                "driver": "bridge",
                "ipam": {"config": [{"subnet": "10.5.0.0/24"}]}
            }
        }
    }

    for service, (node_type, port) in nodes.items():
        compose["services"][f"{cluster_name}-{node_type}-{port}"] = {
            "image": "fedlearn/runtime:1.4.0",
            "ports": [f"{port}:{port}"],
            "volumes": [
                f"{CONFIG_DIR / f'{node_type}-{port}.json'}:/app/config.json",
                "shared_models:/app/models",
                "training_data:/app/data"
            ],
            "networks": ["fednet"],
            "environment": {
                "NODE_TYPE": node_type.upper(),
                "CLUSTER_NAME": cluster_name
            },
            "deploy": {
                "resources": {
                    "limits": {"cpus": "2.0", "memory": "4G"}
                }
            }
        }

    with open(f"docker-compose.{cluster_name}.yml", "w") as f:
        json.dump(compose, f, indent=2)

def main():
    parser = argparse.ArgumentParser(description="Initialize Federated Learning Cluster")
    parser.add_argument("--name", required=True, help="Cluster identifier")
    parser.add_argument("--workers", type=int, default=3, help="Number of worker nodes")
    parser.add_argument("--port-range", nargs=2, type=int, default=DEFAULT_PORTS,
                        help="Port range for cluster nodes")
    parser.add_argument("--output", type=Path, default=Path.cwd(),
                        help="Output directory for configurations")
    args = parser.parse_args()

    try:
        args.output.mkdir(exist_ok=True)
        CERTS_DIR.mkdir(exist_ok=True)
        CONFIG_DIR.mkdir(exist_ok=True)

        validate_environment()

        coordinator_port = find_available_port(*args.port_range)
        nodes = {"coordinator": ("coordinator", coordinator_port)}

        for i in range(1, args.workers + 1):
            worker_port = find_available_port(args.port_range[0], args.port_range[1])
            nodes[f"worker-{i}"] = ("worker", worker_port)

        for node_id, (node_type, port) in nodes.items():
            key_path = CERTS_DIR / f"{node_type}-{port}.key"
            cert_path = CERTS_DIR / f"{node_type}-{port}.crt"
            
            generate_rsa_keypair(key_path)
            generate_self_signed_cert(key_path, cert_path, node_id)

            if node_type == "coordinator":
                config = generate_coordinator_config(args.name, port)
            else:
                config = generate_worker_config(args.name, port, node_id.split("-")[-1])

            config_path = CONFIG_DIR / f"{node_type}-{port}.json"
            with open(config_path, "w") as f:
                json.dump(config, f, indent=2)

        generate_docker_compose(args.name, nodes)
        print(f"Cluster initialization complete. Configurations saved to {args.output}")

    except Exception as e:
        print(f"Initialization failed: {str(e)}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
