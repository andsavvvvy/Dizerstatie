from flask import Flask, render_template, request, redirect, url_for, jsonify, flash, Response
import requests, sys, os, io
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from db.repository import DistributedRepository

app = Flask(__name__)
app.secret_key = os.getenv('FLASK_SECRET_KEY', 'distributed_clustering_secret')
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024

ORCHESTRATOR_URL = os.getenv('ORCHESTRATOR_URL', 'http://localhost:7000')
NODES = [
    {'url': 'http://localhost:6001', 'name': 'Medical Node', 'type': 'healthcare', 'node_id': 'medical_bucharest_01'},
    {'url': 'http://localhost:6002', 'name': 'Retail Node', 'type': 'retail', 'node_id': 'retail_bucharest_01'},
    {'url': 'http://localhost:6003', 'name': 'IoT Node', 'type': 'iot', 'node_id': 'iot_bucharest_01'},
]
ALLOWED_EXT = {'csv', 'xlsx', 'xls', 'json'}


def check_orch():
    try: return requests.get(f'{ORCHESTRATOR_URL}/health', timeout=1).status_code == 200
    except: return False

def check_node(url):
    try: return 'healthy' if requests.get(f'{url}/health', timeout=1).status_code == 200 else 'unhealthy'
    except: return 'unreachable'

def get_node_status(url):
    try:
        r = requests.get(f'{url}/status', timeout=2)
        return r.json() if r.status_code == 200 else None
    except: return None

def get_all_health_parallel():
    results = {}
    def _co(): results['orch'] = check_orch()
    def _cn(n): return {'name': n['name'], 'type': n['type'], 'url': n['url'], 'status': check_node(n['url'])}
    with ThreadPoolExecutor(max_workers=len(NODES) + 1) as ex:
        ex.submit(_co)
        fs = {ex.submit(_cn, n): n for n in NODES}
        ns = [f.result() for f in as_completed(fs)]
    order = {n['name']: i for i, n in enumerate(NODES)}
    ns.sort(key=lambda x: order.get(x['name'], 99))
    return results.get('orch', False), ns

def get_all_nodes_detailed_parallel():
    def _f(n): return {'name': n['name'], 'type': n['type'], 'url': n['url'],
                       'health': check_node(n['url']), 'status': get_node_status(n['url'])}
    with ThreadPoolExecutor(max_workers=len(NODES)) as ex:
        rs = [f.result() for f in as_completed([ex.submit(_f, n) for n in NODES])]
    order = {n['name']: i for i, n in enumerate(NODES)}
    rs.sort(key=lambda x: order.get(x['name'], 99))
    return rs

def parse_upload(f):
    import pandas as pd
    ext = f.filename.rsplit('.', 1)[1].lower()
    raw = f.read()
    if ext == 'csv': df = pd.read_csv(io.BytesIO(raw))
    elif ext in ('xlsx', 'xls'): df = pd.read_excel(io.BytesIO(raw))
    elif ext == 'json': df = pd.read_json(io.BytesIO(raw))
    else: raise ValueError(f'Unsupported: {ext}')
    cols = {'columns': list(df.columns), 'dtypes': {c: str(t) for c, t in df.dtypes.items()},
            'numeric_columns': list(df.select_dtypes(include='number').columns)}
    return {'columns_info': cols, 'row_count': len(df), 'file_type': ext, 'file_bytes': raw}

def build_sensitivity_data(local_by_node, algo_scores):
    algo_node_matrix = []
    for node_id, algos in local_by_node.items():
        for lr in algos:
            algo_node_matrix.append({
                'node_id': node_id,
                'algorithm': lr['algorithm'],
                'silhouette': lr.get('silhouette_score', 0) or 0,
                'davies_bouldin': lr.get('davies_bouldin_score', 0) or 0,
                'n_clusters': lr.get('n_local_clusters', 0),
                'execution_time_ms': lr.get('execution_time_ms', 0),
            })

    quality_speed = []
    for item in algo_node_matrix:
        quality_speed.append({
            'algorithm': item['algorithm'],
            'node_id': item['node_id'],
            'silhouette': item['silhouette'],
            'time_ms': item['execution_time_ms'],
            'clusters': item['n_clusters'],
        })

    algo_distributions = {}
    for a, s in algo_scores.items():
        algo_distributions[a] = {
            'avg': s.get('avg_silhouette', 0),
            'min': s.get('min_silhouette', 0),
            'max': s.get('max_silhouette', 0),
            'std': s.get('std_silhouette', 0),
        }

    return {
        'algo_node_matrix': algo_node_matrix,
        'quality_speed': quality_speed,
        'algo_distributions': algo_distributions,
    }


@app.route('/favicon.ico')
def favicon(): return ('', 204)

@app.route('/')
def index():
    oh, ns = get_all_health_parallel()
    try: ra = DistributedRepository.list_recent_analyses(5)
    except: ra = []
    return render_template('index.html', orchestrator_healthy=oh, nodes_status=ns, recent_analyses=ra)

@app.route('/nodes')
def nodes():
    return render_template('nodes.html', nodes=get_all_nodes_detailed_parallel())

@app.route('/analyses')
def analyses_list():
    try: return render_template('analyses_list.html', analyses=DistributedRepository.list_recent_analyses(50))
    except Exception as e: flash(str(e), 'error'); return render_template('analyses_list.html', analyses=[])

@app.route('/analysis/new')
def analysis_new():
    return render_template('analysis_new.html', nodes=NODES)

@app.route('/analysis/<session_id>/monitor')
def analysis_monitor(session_id):
    return render_template('analysis_monitor.html', session_id=session_id)

@app.route('/analysis/<session_id>')
def analysis_detail(session_id):
    try: analysis = DistributedRepository.get_global_analysis(session_id)
    except Exception as e: flash(str(e), 'error'); return redirect(url_for('analyses_list'))
    if not analysis: flash('Not found', 'error'); return redirect(url_for('analyses_list'))
    if analysis.get('status') not in ('completed', 'failed'):
        return redirect(url_for('analysis_monitor', session_id=session_id))

    np_list, perf_list, lr_list = [], [], []
    try: np_list = DistributedRepository.get_node_participation(session_id)
    except: pass
    try: perf_list = DistributedRepository.get_node_performance(session_id)
    except: pass
    try: lr_list = DistributedRepository.get_local_results_for_session(session_id)
    except: pass

    local_by_node = {}
    for lr in lr_list: local_by_node.setdefault(lr['node_id'], []).append(lr)
    perf_by_node = {p['node_id']: p for p in perf_list}

    ensemble = analysis.get('ensemble_analysis', {})
    algo_scores = ensemble.get('algorithm_scores', {})
    sensitivity = build_sensitivity_data(local_by_node, algo_scores)

    return render_template('analysis_detail.html',
        analysis=analysis, node_participation=np_list,
        node_performance=perf_by_node, local_by_node=local_by_node,
        sensitivity=sensitivity)

@app.route('/analysis/<session_id>/delete', methods=['POST'])
def analysis_delete(session_id):
    try: DistributedRepository.delete_analysis(session_id); flash('Deleted', 'success')
    except Exception as e: flash(str(e), 'error')
    return redirect(request.form.get('next') or url_for('analyses_list'))

@app.route('/analysis/<session_id>/export_pdf')
def analysis_export_pdf(session_id):
    try:
        from ui.pdf_generator import generate_analysis_pdf
    except ImportError:
        try:
            from pdf_generator import generate_analysis_pdf
        except ImportError:
            flash('PDF generator not available. Install reportlab: pip install reportlab', 'error')
            return redirect(url_for('analysis_detail', session_id=session_id))

    try:
        analysis = DistributedRepository.get_global_analysis(session_id)
        if not analysis:
            flash('Analysis not found', 'error')
            return redirect(url_for('analyses_list'))

        np_list = []
        perf_by_node = {}
        local_by_node = {}

        try: np_list = DistributedRepository.get_node_participation(session_id)
        except: pass
        try:
            perf_list = DistributedRepository.get_node_performance(session_id)
            perf_by_node = {p['node_id']: p for p in perf_list}
        except: pass
        try:
            lr_list = DistributedRepository.get_local_results_for_session(session_id)
            for lr in lr_list: local_by_node.setdefault(lr['node_id'], []).append(lr)
        except: pass

        pdf_bytes = generate_analysis_pdf(
            analysis=analysis,
            node_participation=np_list,
            local_by_node=local_by_node,
            node_performance=perf_by_node,
        )

        filename = f"analysis_report_{session_id[:8]}.pdf"
        return Response(
            pdf_bytes,
            mimetype='application/pdf',
            headers={'Content-Disposition': f'attachment; filename="{filename}"'},
        )

    except Exception as e:
        flash(f'PDF generation failed: {e}', 'error')
        return redirect(url_for('analysis_detail', session_id=session_id))


@app.route('/datasets')
def datasets_page():
    ds = DistributedRepository.list_datasets()
    asg = DistributedRepository.get_all_node_assignments()
    by_node = {}
    for a in asg: by_node.setdefault(a['node_id'], []).append(a)
    return render_template('datasets.html', datasets=ds, nodes=NODES, assignments_by_node=by_node)

@app.route('/datasets/upload', methods=['POST'])
def dataset_upload():
    if 'file' not in request.files or request.files['file'].filename == '':
        flash('No file', 'error'); return redirect(url_for('datasets_page'))
    f = request.files['file']
    if '.' not in f.filename or f.filename.rsplit('.', 1)[1].lower() not in ALLOWED_EXT:
        flash('Invalid type', 'error'); return redirect(url_for('datasets_page'))
    name = request.form.get('name', '').strip() or f.filename
    try:
        p = parse_upload(f)
        DistributedRepository.save_dataset(name=name, original_filename=f.filename,
            file_type=p['file_type'], file_data=p['file_bytes'],
            columns_info=p['columns_info'], row_count=p['row_count'],
            description=request.form.get('description', '').strip())
        flash(f'"{name}" uploaded ({p["row_count"]} rows)', 'success')
    except Exception as e: flash(str(e), 'error')
    return redirect(url_for('datasets_page'))

@app.route('/datasets/<int:did>/delete', methods=['POST'])
def dataset_delete(did):
    try: DistributedRepository.delete_dataset(did); flash('Deleted', 'success')
    except Exception as e: flash(str(e), 'error')
    return redirect(url_for('datasets_page'))

@app.route('/datasets/<int:did>/download')
def dataset_download(did):
    r = DistributedRepository.get_dataset_file(did)
    if not r: flash('Not found', 'error'); return redirect(url_for('datasets_page'))
    data, fn, ft = r
    mt = {'csv': 'text/csv', 'xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
          'json': 'application/json'}.get(ft, 'application/octet-stream')
    return Response(data, mimetype=mt, headers={'Content-Disposition': f'attachment; filename="{fn}"'})

@app.route('/datasets/assign', methods=['POST'])
def dataset_assign():
    nid, did = request.form.get('node_id'), request.form.get('dataset_id', type=int)
    act = request.form.get('set_active') == '1'
    if not nid or not did: flash('Missing', 'error'); return redirect(url_for('datasets_page'))
    try: DistributedRepository.assign_dataset_to_node(nid, did, set_active=act); flash('Assigned', 'success')
    except Exception as e: flash(str(e), 'error')
    return redirect(url_for('datasets_page'))

@app.route('/datasets/set_active', methods=['POST'])
def dataset_set_active():
    nid, did = request.form.get('node_id'), request.form.get('dataset_id', type=int)
    try: DistributedRepository.set_active_dataset(nid, did); flash('Updated', 'success')
    except Exception as e: flash(str(e), 'error')
    return redirect(url_for('datasets_page'))

@app.route('/datasets/unassign', methods=['POST'])
def dataset_unassign():
    try: DistributedRepository.unassign_dataset(request.form['node_id'], int(request.form['dataset_id']))
    except Exception as e: flash(str(e), 'error')
    return redirect(url_for('datasets_page'))


@app.route('/analysis/start', methods=['POST'])
def analysis_start():
    try:
        r = requests.post(f'{ORCHESTRATOR_URL}/analysis/start',
            json={'expected_nodes': len(NODES), 'description': request.form.get('description') or 'Manual'}, timeout=10)
        r.raise_for_status(); d = r.json()
        return jsonify({'status': 'success', 'session_id': d['session_id'],
                        'redirect': url_for('analysis_monitor', session_id=d['session_id'])})
    except Exception as e: return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/analysis/<sid>/trigger_nodes', methods=['POST'])
def analysis_trigger_nodes(sid):
    def call(n):
        try:
            r = requests.post(f"{n['url']}/send_to_global", json={'session_id': sid}, timeout=600)
            return {'node': n['name'], 'status': 'success' if r.status_code == 200 else 'error'}
        except Exception as e: return {'node': n['name'], 'status': 'error', 'message': str(e)}
    rs = []
    with ThreadPoolExecutor(max_workers=len(NODES)) as ex:
        for f in as_completed([ex.submit(call, n) for n in NODES]): rs.append(f.result())
    rs.sort(key=lambda x: x['node'])
    return jsonify({'status': 'completed', 'results': rs})

@app.route('/analysis/<sid>/aggregate', methods=['POST'])
def analysis_aggregate(sid):
    try:
        r = requests.post(f'{ORCHESTRATOR_URL}/analysis/{sid}/aggregate', timeout=180)
        r.raise_for_status(); return jsonify(r.json())
    except Exception as e: return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/analysis/<sid>/status')
def api_analysis_status(sid):
    try:
        r = requests.get(f'{ORCHESTRATOR_URL}/analysis/{sid}/status', timeout=5)
        return jsonify(r.json()) if r.status_code == 200 else (jsonify({'status': 'error'}), 500)
    except Exception as e: return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/api/system/health')
def api_system_health():
    o = check_orch()
    nh = {n['name']: check_node(n['url']) for n in NODES}
    return jsonify({
        'overall': 'healthy' if o and all(s == 'healthy' for s in nh.values()) else 'degraded',
        'orchestrator': 'healthy' if o else 'unhealthy',
        'nodes': nh
    })

@app.route('/api/node/test')
def api_node_test():
    url = request.args.get('url', '')
    allowed = {n['url'] for n in NODES}
    if url not in allowed:
        return jsonify({'status': 'error', 'message': 'Unknown node URL'}), 400
    try:
        r = requests.get(f'{url}/health', timeout=2)
        data = r.json()
        return jsonify({'status': data.get('status', 'unknown'), 'node_id': data.get('node_id', '')})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500


@app.errorhandler(404)
def e404(_): return render_template('404.html'), 404

@app.errorhandler(500)
def e500(_): return render_template('500.html'), 500

@app.template_filter('datetime')
def fmt_dt(v):
    if not v: return 'N/A'
    if isinstance(v, str):
        try: v = datetime.fromisoformat(v.replace('Z', '+00:00'))
        except: return v
    return v.strftime('%Y-%m-%d %H:%M:%S')

@app.template_filter('duration')
def fmt_dur(ms):
    if not ms: return 'N/A'
    s = ms / 1000
    if s < 60: return f'{s:.1f}s'
    if s < 3600: return f'{s/60:.1f}m'
    return f'{s/3600:.1f}h'

@app.template_filter('number')
def fmt_num(v): return f'{v:,}' if v else 'N/A'

@app.template_filter('filesize')
def fmt_fs(b):
    if not b: return 'N/A'
    if b < 1024: return f'{b} B'
    if b < 1048576: return f'{b/1024:.1f} KB'
    return f'{b/1048576:.1f} MB'


if __name__ == '__main__':
    print("  Web Interface: http://localhost:9000")
    app.run(host='0.0.0.0', port=9000, debug=True, threaded=True)