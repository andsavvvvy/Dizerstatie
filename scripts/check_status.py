#!/usr/bin/env python3
"""
Quick status check for all services
"""
import requests

SERVICES = [
    ('Global Orchestrator', 'http://localhost:7000/health'),
    ('Medical Node', 'http://localhost:6001/health'),
    ('Retail Node', 'http://localhost:6002/health'),
    ('IoT Node', 'http://localhost:6003/health'),
]

print("\n" + "="*60)
print("  DISTRIBUTED CLUSTERING - SYSTEM STATUS")
print("="*60 + "\n")

for name, url in SERVICES:
    try:
        response = requests.get(url, timeout=3)
        if response.status_code == 200:
            print(f"✓ {name:25s} - HEALTHY")
        else:
            print(f"✗ {name:25s} - UNHEALTHY (status {response.status_code})")
    except:
        print(f"✗ {name:25s} - UNREACHABLE")

print("\n" + "="*60 + "\n")