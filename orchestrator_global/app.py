"""
Global Orchestrator API
Coordinates distributed clustering across multiple nodes
"""
from flask import Flask, request, jsonify
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from orchestrator_global.aggregation_engine import GlobalAggregationEngine
from db.repository import DistributedRepository
import uuid
from datetime import datetime, date
import time
import logging
import json


class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return super().default(obj)


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.json_encoder = DateTimeEncoder

# In-memory session store
active_sessions = {}


# ============================================
# Health & Info
# ============================================

@app.route('/health', methods=['GET'])
def health():
    return jsonify({
        'status': 'healthy',
        'service': 'global_orchestrator',
        'timestamp': datetime.now().isoformat(),
        'active_sessions': len(active_sessions),
        'version': '1.0.0',
    })


@app.route('/info', methods=['GET'])
def info():
    return jsonify({
        'service': 'Global Orchestrator',
        'description': 'Coordinates distributed data mining across multiple nodes',
        'endpoints': {
            'health': 'GET /health',
            'analysis_start': 'POST /analysis/start',
            'analysis_receive': 'POST /analysis/<session_id>/receive',
            'analysis_aggregate': 'POST /analysis/<session_id>/aggregate',
            'analysis_status': 'GET /analysis/<session_id>/status',
            'analysis_recent': 'GET /analysis/recent',
        },
    })


# ============================================
# Node Management
# ============================================

@app.route('/nodes/register', methods=['POST'])
def register_node():
    try:
        node_config = request.json
        required = ['node_id', 'node_type']
        for field in required:
            if field not in node_config:
                return jsonify({'status': 'error', 'message': f'Missing: {field}'}), 400

        node_db_id = DistributedRepository.register_node(node_config)
        logger.info(f"✓ Registered node: {node_config['node_id']}")

        return jsonify({
            'status': 'registered',
            'node_id': node_config['node_id'],
            'db_id': node_db_id,
        })
    except Exception as e:
        logger.error(f"Node registration failed: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500


@app.route('/nodes/list', methods=['GET'])
def list_nodes():
    try:
        nodes = DistributedRepository.get_all_nodes()
        return jsonify({'status': 'success', 'count': len(nodes), 'nodes': nodes})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500


@app.route('/nodes/<node_id>/heartbeat', methods=['POST'])
def node_heartbeat(node_id):
    try:
        DistributedRepository.update_node_heartbeat(node_id)
        return jsonify({'status': 'success', 'node_id': node_id})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500


# ============================================
# Analysis Workflow
# ============================================

@app.route('/analysis/start', methods=['POST'])
def start_analysis():
    try:
        params = request.json or {}
        expected_nodes = params.get('expected_nodes', 3)
        description = params.get('description', 'Distributed clustering analysis')

        session_id = str(uuid.uuid4())
        engine = GlobalAggregationEngine()

        active_sessions[session_id] = {
            'engine': engine,
            'expected_nodes': expected_nodes,
            'received_nodes': 0,
            'start_time': time.time(),
            'status': 'waiting_for_nodes',
            'description': description,
        }

        DistributedRepository.create_global_analysis(
            session_id=session_id,
            total_nodes=expected_nodes,
            total_points=0,
        )

        logger.info(
            f"✓ Started new analysis session: {session_id} "
            f"(expecting {expected_nodes} nodes)"
        )

        return jsonify({
            'status': 'initialized',
            'session_id': session_id,
            'expected_nodes': expected_nodes,
            'submit_endpoint': f'/analysis/{session_id}/receive',
            'status_endpoint': f'/analysis/{session_id}/status',
            'aggregate_endpoint': f'/analysis/{session_id}/aggregate',
        }), 201

    except Exception as e:
        logger.error(f"Failed to start analysis: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500


@app.route('/analysis/<session_id>/receive', methods=['POST'])
def receive_local_results(session_id):
    try:
        if session_id not in active_sessions:
            return jsonify({'status': 'error', 'message': 'Invalid or expired session_id'}), 404

        node_result = request.json
        session = active_sessions[session_id]

        if 'node_id' not in node_result:
            return jsonify({'status': 'error', 'message': 'Missing node_id'}), 400

        session['engine'].receive_node_results(node_result)
        session['received_nodes'] += 1

        DistributedRepository.save_local_results(
            session_id=session_id,
            node_id=node_result['node_id'],
            results=node_result['results'],
        )

        DistributedRepository.update_node_heartbeat(node_result['node_id'])

        if session['received_nodes'] >= session['expected_nodes']:
            session['status'] = 'ready_for_aggregation'
            logger.info(f"✓ Session {session_id} ready for aggregation")

        logger.info(
            f"✓ Received results from {node_result['node_id']} "
            f"({session['received_nodes']}/{session['expected_nodes']})"
        )

        return jsonify({
            'status': 'received',
            'session_id': session_id,
            'node_id': node_result['node_id'],
            'received_nodes': session['received_nodes'],
            'expected_nodes': session['expected_nodes'],
            'ready_for_aggregation': session['status'] == 'ready_for_aggregation',
        })

    except Exception as e:
        logger.error(f"Failed to receive results: {e}")
        return jsonify({'status': 'error', 'message': str(e)}), 500


@app.route('/analysis/<session_id>/aggregate', methods=['POST'])
def trigger_aggregation(session_id):
    try:
        if session_id not in active_sessions:
            db_analysis = DistributedRepository.get_global_analysis(session_id)
            if db_analysis:
                return jsonify({
                    'status': 'already_completed',
                    'session_id': session_id,
                    'report': db_analysis,
                })
            return jsonify({'status': 'error', 'message': 'Invalid or expired session_id'}), 404

        session = active_sessions[session_id]

        if session['received_nodes'] < session['expected_nodes']:
            return jsonify({
                'status': 'waiting',
                'received_nodes': session['received_nodes'],
                'expected_nodes': session['expected_nodes'],
                'message': f"Waiting for {session['expected_nodes'] - session['received_nodes']} more nodes",
            }), 202

        logger.info(f"Starting global aggregation for session {session_id}")

        start_time = time.time()
        report = session['engine'].generate_global_report()
        execution_time_ms = int((time.time() - start_time) * 1000)

        # Extract node system metrics from the report
        node_system_metrics = report.get('metadata', {}).get('node_system_metrics', {})

        DistributedRepository.complete_global_analysis(
            session_id=session_id,
            report=report,
            execution_time_ms=execution_time_ms,
            node_system_metrics=node_system_metrics,
        )

        session['status'] = 'completed'
        session['report'] = report
        session['completion_time'] = time.time()

        logger.info(
            f"✓ Global aggregation completed for {session_id} "
            f"in {execution_time_ms}ms"
        )

        return jsonify({
            'status': 'completed',
            'session_id': session_id,
            'execution_time_ms': execution_time_ms,
            'report': report,
        })

    except Exception as e:
        logger.error(f"Aggregation failed: {e}")
        if session_id in active_sessions:
            active_sessions[session_id]['status'] = 'failed'
            active_sessions[session_id]['error'] = str(e)
        return jsonify({'status': 'error', 'message': str(e)}), 500


@app.route('/analysis/<session_id>/status', methods=['GET'])
def analysis_status(session_id):
    try:
        if session_id in active_sessions:
            session = active_sessions[session_id]
            elapsed = int(time.time() - session['start_time'])

            response = {
                'session_id': session_id,
                'status': session['status'],
                'received_nodes': session['received_nodes'],
                'expected_nodes': session['expected_nodes'],
                'elapsed_seconds': elapsed,
                'description': session.get('description', ''),
            }

            if session['status'] == 'completed':
                response['completion_time'] = session.get('completion_time')
                response['report_available'] = True

            return jsonify(response)

        db_analysis = DistributedRepository.get_global_analysis(session_id)
        if db_analysis:
            return jsonify({
                'session_id': session_id,
                'status': db_analysis['status'],
                'from_database': True,
            })

        return jsonify({'status': 'not_found', 'session_id': session_id}), 404

    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500


@app.route('/analysis/recent', methods=['GET'])
def recent_analyses():
    try:
        limit = int(request.args.get('limit', 10))
        analyses = DistributedRepository.list_recent_analyses(limit)
        return jsonify({'status': 'success', 'count': len(analyses), 'analyses': analyses})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500


@app.route('/analysis/<session_id>/report', methods=['GET'])
def get_report(session_id):
    try:
        if session_id in active_sessions:
            session = active_sessions[session_id]
            if session['status'] == 'completed':
                return jsonify({'status': 'success', 'report': session['report']})
            return jsonify({'status': 'not_ready', 'message': f"Status: {session['status']}"}), 202

        db_analysis = DistributedRepository.get_global_analysis(session_id)
        if db_analysis and db_analysis['status'] == 'completed':
            return jsonify({'status': 'success', 'report': db_analysis})

        return jsonify({'status': 'not_found'}), 404

    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500


# ============================================
# Error Handlers
# ============================================

@app.errorhandler(404)
def not_found(_):
    return jsonify({'status': 'error', 'message': 'Endpoint not found'}), 404


@app.errorhandler(500)
def internal_error(_):
    return jsonify({'status': 'error', 'message': 'Internal server error'}), 500


# ============================================
# Startup
# ============================================

if __name__ == '__main__':
    print("""
    ========================================
      GLOBAL ORCHESTRATOR
      Distributed Data Mining Coordinator
      Port: 7000
      Status: http://localhost:7000/health
    ========================================
    """)

    app.run(host='0.0.0.0', port=7000, debug=False, threaded=True)