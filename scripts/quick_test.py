#!/usr/bin/env python3
"""
Quick test script to verify all components are working
"""
import requests
import time

SERVICES = {
    'Global Orchestrator': 'http://localhost:7000/health',
    'Medical Node': 'http://localhost:6001/health',
    'Retail Node': 'http://localhost:6002/health',
    'IoT Node': 'http://localhost:6003/health',
    'Web UI': 'http://localhost:9000',
}

def test_service(name, url):
    """Test if a service is responsive"""
    try:
        response = requests.get(url, timeout=3)
        if response.status_code == 200:
            print(f"✓ {name:25s} - OK")
            return True
        else:
            print(f"✗ {name:25s} - HTTP {response.status_code}")
            return False
    except requests.exceptions.ConnectionError:
        print(f"✗ {name:25s} - CONNECTION REFUSED")
        return False
    except requests.exceptions.Timeout:
        print(f"✗ {name:25s} - TIMEOUT")
        return False
    except Exception as e:
        print(f"✗ {name:25s} - ERROR: {e}")
        return False

def test_quick_analysis():
    """Test a quick end-to-end analysis"""
    print("\n" + "="*60)
    print("  QUICK END-TO-END TEST")
    print("="*60 + "\n")
    
    try:
        # 1. Start analysis
        print("1. Starting analysis...")
        response = requests.post(
            'http://localhost:7000/analysis/start',
            json={'expected_nodes': 3},
            timeout=10
        )
        response.raise_for_status()
        session_id = response.json()['session_id']
        print(f"   ✓ Session ID: {session_id[:16]}...")
        
        # 2. Trigger nodes
        print("\n2. Triggering nodes...")
        for node_url in ['http://localhost:6001', 'http://localhost:6002', 'http://localhost:6003']:
            response = requests.post(
                f"{node_url}/send_to_global",
                json={'session_id': session_id},
                timeout=120
            )
            response.raise_for_status()
            print(f"   ✓ Node submitted")
        
        # 3. Wait a bit
        print("\n3. Waiting for nodes...")
        time.sleep(3)
        
        # 4. Trigger aggregation
        print("\n4. Triggering aggregation...")
        response = requests.post(
            f'http://localhost:7000/analysis/{session_id}/aggregate',
            timeout=180
        )
        response.raise_for_status()
        result = response.json()
        
        if result['status'] == 'completed':
            print(f"\n✓ ANALYSIS COMPLETED!")
            print(f"  Best Algorithm: {result['report']['ensemble_analysis']['best_algorithm']}")
            print(f"  Execution Time: {result['execution_time_ms']}ms")
            print(f"\n  View results: http://localhost:9000/analysis/{session_id}")
            return True
        else:
            print(f"\n✗ Analysis failed: {result.get('message')}")
            return False
            
    except Exception as e:
        print(f"\n✗ Test failed: {e}")
        return False

def main():
    print("\n" + "="*60)
    print("  DISTRIBUTED CLUSTERING SYSTEM - QUICK TEST")
    print("="*60 + "\n")
    
    print("Testing Services:")
    print("-" * 60)
    
    all_ok = True
    for name, url in SERVICES.items():
        if not test_service(name, url):
            all_ok = False
    
    print("-" * 60)
    
    if not all_ok:
        print("\n✗ Some services are not responding!")
        print("  Make sure all services are started:")
        print("  .\\scripts\\start_distributed.ps1")
        return False
    
    print("\n✓ All services are running!")
    
    # Ask if user wants to run end-to-end test
    response = input("\nRun quick end-to-end analysis test? (y/N): ")
    
    if response.lower() == 'y':
        success = test_quick_analysis()
        
        if success:
            print("\n" + "="*60)
            print("  ALL TESTS PASSED! ✓")
            print("="*60)
        else:
            print("\n" + "="*60)
            print("  END-TO-END TEST FAILED! ✗")
            print("="*60)
    
    print("\n")

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nTest interrupted by user.")