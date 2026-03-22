#!/usr/bin/env python3
"""
Distributed Clustering - Full Analysis Script

Orchestrates complete distributed analysis workflow:
1. Check node health
2. Initialize global analysis
3. Trigger local clustering on all nodes
4. Wait for results
5. Trigger global aggregation
6. Display and save report

Usage:
    python scripts/run_full_analysis.py
"""

import requests
import time
import json
import os
from datetime import datetime
from typing import List, Dict, Any
import sys
NODES = [
    {
        'url': 'http://localhost:6001',
        'name': 'Medical Node',
        'node_id': 'medical_bucharest_01',
        'type': 'healthcare'
    },
    {
        'url': 'http://localhost:6002',
        'name': 'Retail Node',
        'node_id': 'retail_bucharest_01',
        'type': 'retail'
    },
    {
        'url': 'http://localhost:6003',
        'name': 'IoT Node',
        'node_id': 'iot_bucharest_01',
        'type': 'iot'
    }
]

GLOBAL_ORCHESTRATOR = 'http://localhost:7000'
class Colors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def print_header(text: str):
    """Print section header"""
    print(f"\n{Colors.CYAN}{'='*70}{Colors.ENDC}")
    print(f"{Colors.CYAN}{Colors.BOLD}  {text}{Colors.ENDC}")
    print(f"{Colors.CYAN}{'='*70}{Colors.ENDC}\n")

def print_success(text: str):
    """Print success message"""
    print(f"{Colors.GREEN}✓ {text}{Colors.ENDC}")

def print_error(text: str):
    """Print error message"""
    print(f"{Colors.RED}✗ {text}{Colors.ENDC}")

def print_info(text: str):
    """Print info message"""
    print(f"{Colors.BLUE}ℹ {text}{Colors.ENDC}")

def print_warning(text: str):
    """Print warning message"""
    print(f"{Colors.YELLOW}⚠ {text}{Colors.ENDC}")

def check_orchestrator_health() -> bool:
    """Check if global orchestrator is running"""
    try:
        response = requests.get(f"{GLOBAL_ORCHESTRATOR}/health", timeout=5)
        if response.status_code == 200:
            print_success("Global Orchestrator is healthy")
            return True
        else:
            print_error(f"Global Orchestrator unhealthy (status {response.status_code})")
            return False
    except Exception as e:
        print_error(f"Cannot reach Global Orchestrator: {e}")
        return False

def check_nodes_health() -> Dict[str, bool]:
    """Check health of all nodes"""
    print_header("STEP 1: Checking Node Health")
    
    results = {}
    all_healthy = True
    
    for node in NODES:
        try:
            response = requests.get(f"{node['url']}/health", timeout=5)
            
            if response.status_code == 200:
                print_success(f"{node['name']:20s} - HEALTHY")
                results[node['node_id']] = True
            else:
                print_error(f"{node['name']:20s} - UNHEALTHY (status {response.status_code})")
                results[node['node_id']] = False
                all_healthy = False
                
        except Exception as e:
            print_error(f"{node['name']:20s} - UNREACHABLE ({str(e)[:50]})")
            results[node['node_id']] = False
            all_healthy = False
    
    return results, all_healthy

def start_global_analysis() -> str:
    """Initialize global analysis session"""
    print_header("STEP 2: Initializing Global Analysis")
    
    try:
        response = requests.post(
            f"{GLOBAL_ORCHESTRATOR}/analysis/start",
            json={
                'expected_nodes': len(NODES),
                'description': f'Automated analysis at {datetime.now().isoformat()}'
            },
            timeout=10
        )
        
        response.raise_for_status()
        data = response.json()
        
        session_id = data['session_id']
        
        print_success("Global analysis initialized")
        print_info(f"  Session ID: {session_id}")
        print_info(f"  Expected nodes: {data['expected_nodes']}")
        
        return session_id
        
    except Exception as e:
        print_error(f"Failed to start analysis: {e}")
        sys.exit(1)

def trigger_node_clustering(session_id: str) -> Dict[str, Any]:
    """Trigger local clustering on all nodes"""
    print_header("STEP 3: Triggering Local Clustering on Nodes")
    
    results = {}
    
    for i, node in enumerate(NODES, 1):
        print(f"\n[{i}/{len(NODES)}] {Colors.BOLD}{node['name']}{Colors.ENDC}")
        print("-" * 50)
        
        try:
            print_info("Running local clustering...")
            
            start_time = time.time()
            
            response = requests.post(
                f"{node['url']}/send_to_global",
                json={'session_id': session_id},
                timeout=120
            )
            
            elapsed = time.time() - start_time
            
            response.raise_for_status()
            data = response.json()
            
            print_success(f"Local clustering completed ({elapsed:.1f}s)")
            
            if 'global_response' in data:
                gr = data['global_response']
                print_info(f"  Submitted to orchestrator: {gr.get('status', 'unknown')}")
                print_info(f"  Nodes received: {gr.get('received_nodes', 0)}/{gr.get('expected_nodes', 0)}")
            
            results[node['node_id']] = {
                'status': 'success',
                'time': elapsed
            }
            
            time.sleep(0.5)  # Small delay between nodes
            
        except requests.exceptions.Timeout:
            print_error("Timeout waiting for node response")
            results[node['node_id']] = {'status': 'timeout'}
            
        except Exception as e:
            print_error(f"Error: {str(e)[:100]}")
            results[node['node_id']] = {'status': 'error', 'message': str(e)}
    
    return results

def wait_for_aggregation_ready(session_id: str, max_wait: int = 60) -> bool:
    """Wait until all nodes have submitted results"""
    print_header("STEP 4: Waiting for All Nodes to Submit")
    
    start_time = time.time()
    dots = 0
    
    while time.time() - start_time < max_wait:
        try:
            response = requests.get(
                f"{GLOBAL_ORCHESTRATOR}/analysis/{session_id}/status",
                timeout=10
            )
            
            if response.status_code == 200:
                data = response.json()
                status = data.get('status')
                
                if status == 'ready_for_aggregation':
                    print_success("\nAll nodes submitted results!")
                    return True
                
                elif status == 'completed':
                    print_success("\nAnalysis already completed!")
                    return True
                received = data.get('received_nodes', 0)
                expected = data.get('expected_nodes', 0)
                elapsed = int(time.time() - start_time)
                dots = (dots + 1) % 4
                dot_str = '.' * dots + ' ' * (3 - dots)
                
                print(f"\r  Waiting{dot_str} ({received}/{expected} nodes, {elapsed}s)", end='', flush=True)
                
                time.sleep(2)
            else:
                print_error(f"\nUnexpected status code: {response.status_code}")
                return False
                
        except Exception as e:
            print_error(f"\nError checking status: {e}")
            return False
    
    print_error(f"\nTimeout after {max_wait}s")
    return False

def trigger_global_aggregation(session_id: str) -> Dict[str, Any]:
    """Trigger global aggregation and meta-clustering"""
    print_header("STEP 5: Triggering Global Aggregation")
    
    print_info("Running meta-clustering and ensemble analysis...")
    print_info("This may take a few moments...")
    print()
    
    try:
        start_time = time.time()
        
        response = requests.post(
            f"{GLOBAL_ORCHESTRATOR}/analysis/{session_id}/aggregate",
            timeout=180
        )
        
        elapsed = time.time() - start_time
        
        response.raise_for_status()
        data = response.json()
        
        if data['status'] == 'completed':
            print_success(f"Global aggregation completed ({elapsed:.1f}s)")
            print_info(f"  Execution time: {data['execution_time_ms']}ms")
            return data['report']
        else:
            print_error(f"Aggregation failed: {data.get('message', 'Unknown error')}")
            return None
            
    except Exception as e:
        print_error(f"Failed to aggregate: {e}")
        return None

def display_report(report: Dict[str, Any], session_id: str):
    """Display formatted report"""
    print_header("STEP 6: GLOBAL ANALYSIS REPORT")
    
    metadata = report['metadata']
    ensemble = report['ensemble_analysis']
    print(f"{Colors.BOLD}╔{'═'*68}╗{Colors.ENDC}")
    print(f"{Colors.BOLD}║  DISTRIBUTED DATA MINING ANALYSIS REPORT{' '*25}║{Colors.ENDC}")
    print(f"{Colors.BOLD}╚{'═'*68}╝{Colors.ENDC}\n")
    print(f"{Colors.CYAN}{Colors.BOLD}📊 SYSTEM OVERVIEW{Colors.ENDC}")
    print(f"{'─'*70}")
    print(f"  Total Nodes:       {metadata['total_nodes']}")
    print(f"  Total Data Points: {metadata['total_data_points']:,}")
    print()
    print(f"{Colors.CYAN}{Colors.BOLD}🖥️  NODE SUMMARY{Colors.ENDC}")
    print(f"{'─'*70}")
    for node in metadata['nodes_summary']:
        print(f"\n  {Colors.BOLD}{node['node_id']}{Colors.ENDC}")
        print(f"    Type:       {node['node_type']}")
        print(f"    Samples:    {node['n_samples']:,}")
        print(f"    Features:   {node['n_features']}")
        print(f"    Algorithms: {', '.join(node['algorithms_used'])}")
    print()
    best_algo = ensemble['best_algorithm']
    best_score = ensemble['algorithm_scores'][best_algo]
    
    print(f"{Colors.GREEN}{Colors.BOLD}🏆 BEST ALGORITHM: {best_algo}{Colors.ENDC}")
    print(f"{'─'*70}")
    print(f"  Avg Silhouette:  {best_score['avg_silhouette']:.4f}")
    print(f"  Consistency:     {best_score['consistency']:.4f}")
    print(f"  Avg Clusters:    {best_score['avg_clusters']:.1f}")
    print(f"  Nodes Used:      {best_score['nodes_count']}")
    print()
    print(f"  {Colors.BOLD}Recommendation:{Colors.ENDC}")
    print(f"  {ensemble['recommendation']}")
    print()
    print(f"{Colors.CYAN}{Colors.BOLD}📈 ALGORITHM PERFORMANCE (Across All Nodes){Colors.ENDC}")
    print(f"{'─'*70}")
    print(f"  {'Algorithm':<12} {'Avg Silhouette':<15} {'Consistency':<12} {'Nodes':<6}")
    print(f"  {'-'*12} {'-'*15} {'-'*12} {'-'*6}")
    
    for algo, scores in sorted(
        ensemble['algorithm_scores'].items(),
        key=lambda x: x[1]['avg_silhouette'],
        reverse=True
    ):
        marker = "★" if algo == best_algo else " "
        print(f"  {marker} {algo:<10} {scores['avg_silhouette']:<15.4f} {scores['consistency']:<12.4f} {scores['nodes_count']:<6}")
    print()
    if 'insights' in report and report['insights']['cross_org_clusters']:
        print(f"{Colors.YELLOW}{Colors.BOLD}🔗 CROSS-ORGANIZATIONAL INSIGHTS{Colors.ENDC}")
        print(f"{'─'*70}")
        
        for i, insight in enumerate(report['insights']['cross_org_clusters'][:3], 1):
            print(f"\n  {Colors.BOLD}Pattern {i}:{Colors.ENDC}")
            print(f"    Algorithm:      {insight['algorithm']}")
            print(f"    Organizations:  {', '.join(insight['organizations'])}")
            print(f"    Data Points:    {insight['size']:,}")
            print(f"    Cohesion:       {insight['cohesion']:.3f}")
            print(f"    Interpretation: {insight['interpretation']}")
        print()
    save_report(report, session_id)

def save_report(report: Dict[str, Any], session_id: str):
    """Save report to JSON file"""
    reports_dir = 'reports'
    os.makedirs(reports_dir, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"{reports_dir}/analysis_{timestamp}_{session_id[:8]}.json"
    with open(filename, 'w') as f:
        json.dump(report, f, indent=2, default=str)
    
    print(f"{Colors.GREEN}💾 Report saved to: {filename}{Colors.ENDC}")
    print()

def main():
    """Main execution"""
    print(f"\n{Colors.CYAN}{Colors.BOLD}")
    print("╔═══════════════════════════════════════════════════════════════════╗")
    print("║                                                                   ║")
    print("║           DISTRIBUTED DATA MINING SYSTEM                          ║")
    print("║           Full Analysis Orchestration                             ║")
    print("║                                                                   ║")
    print("╚═══════════════════════════════════════════════════════════════════╝")
    print(f"{Colors.ENDC}\n")
    print_header("PRE-FLIGHT CHECKS")
    
    if not check_orchestrator_health():
        print_error("\nGlobal Orchestrator is not running!")
        print_info("Start it with: python orchestrator_global/app.py")
        sys.exit(1)
    
    node_health, all_healthy = check_nodes_health()
    
    if not all_healthy:
        print_warning("\nSome nodes are unhealthy!")
        response = input("Continue anyway? (y/N): ")
        if response.lower() != 'y':
            print_info("Aborted by user")
            sys.exit(0)
    overall_start = time.time()
    
    session_id = start_global_analysis()
    
    trigger_results = trigger_node_clustering(session_id)
    
    if not wait_for_aggregation_ready(session_id):
        print_error("\nNot all nodes completed successfully")
        print_info("Check individual node logs for details")
        sys.exit(1)
    
    report = trigger_global_aggregation(session_id)
    
    if report:
        display_report(report, session_id)
        
        overall_time = time.time() - overall_start
        
        print(f"{Colors.GREEN}{Colors.BOLD}✓ ANALYSIS COMPLETED SUCCESSFULLY!{Colors.ENDC}")
        print(f"  Total time: {overall_time:.1f}s")
        print()
    else:
        print_error("\nAggregation failed!")
        sys.exit(1)

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print(f"\n\n{Colors.YELLOW}⚠ Interrupted by user{Colors.ENDC}\n")
        sys.exit(0)
    except Exception as e:
        print(f"\n{Colors.RED}✗ Unexpected error: {e}{Colors.ENDC}\n")
        sys.exit(1)