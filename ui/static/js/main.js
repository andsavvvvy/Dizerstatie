// Distributed Clustering UI — Main JavaScript

// ============================================
// Toast Notifications
// ============================================
function showToast(message, type) {
    type = type || 'info';
    var c = document.getElementById('toast-container') || createToastContainer();
    var t = document.createElement('div');
    t.className = 'toast align-items-center text-white bg-' + type + ' border-0';
    t.setAttribute('role', 'alert');
    t.innerHTML = '<div class="d-flex"><div class="toast-body">' + message +
        '</div><button type="button" class="btn-close btn-close-white me-2 m-auto" data-bs-dismiss="toast"></button></div>';
    c.appendChild(t);
    new bootstrap.Toast(t, { delay: 3000 }).show();
    t.addEventListener('hidden.bs.toast', function () { t.remove(); });
}
function createToastContainer() {
    var c = document.createElement('div');
    c.id = 'toast-container'; c.className = 'toast-container position-fixed bottom-0 end-0 p-3';
    document.body.appendChild(c); return c;
}

// ============================================
// Utilities
// ============================================
function copyToClipboard(text) {
    navigator.clipboard.writeText(text).then(function () { showToast('Copied!', 'success'); })
        .catch(function () { showToast('Copy failed', 'danger'); });
}

// ============================================
// System Status
// ============================================
function updateSystemStatus() {
    fetch('/api/system/health').then(function (r) { return r.json(); }).then(function (d) {
        var b = document.getElementById('system-status');
        if (!b) return;
        if (d.overall === 'healthy') { b.className = 'badge bg-success'; b.innerHTML = '<i class="bi bi-circle-fill"></i> Online'; }
        else { b.className = 'badge bg-warning'; b.innerHTML = '<i class="bi bi-exclamation-triangle-fill"></i> Degraded'; }
    }).catch(function () {});
}

// ============================================
// Analysis Monitor
// ============================================
function initAnalysisMonitor(sid) {
    var t0 = Date.now();
    var el = document.getElementById('init-time');
    if (el) el.textContent = new Date().toLocaleTimeString();
    setInterval(function () {
        var e = document.getElementById('elapsed-time');
        if (e) e.textContent = Math.floor((Date.now() - t0) / 1000) + 's';
    }, 1000);
    setTimeout(function () { runAnalysis(sid); }, 1000);
}
function addLog(msg, type) {
    var log = document.getElementById('progress-log'); if (!log) return;
    var e = document.createElement('div'); e.className = 'log-entry log-' + (type || 'info');
    e.innerHTML = '<span class="log-time">' + new Date().toLocaleTimeString() + '</span><span class="log-message">' + msg + '</span>';
    log.appendChild(e); log.scrollTop = log.scrollHeight;
}
function setStage(s) {
    var el = document.getElementById('current-stage');
    if (el) el.textContent = s.charAt(0).toUpperCase() + s.slice(1);
    ['init','nodes','aggregate','complete'].forEach(function (st, i, arr) {
        var step = document.getElementById('step-' + st);
        if (step && arr.indexOf(s) >= i) step.classList.add('active');
    });
}
function setErrorState() {
    var el = document.getElementById('analysis-status');
    if (el) { el.className = 'badge bg-danger'; el.textContent = 'Error'; }
}
async function runAnalysis(sid) {
    try {
        setStage('nodes'); addLog('Triggering nodes...', 'info');
        var nr = await fetch('/analysis/' + sid + '/trigger_nodes', { method: 'POST' });
        if (!nr.ok) { addLog('[FAIL] ' + nr.status, 'error'); setErrorState(); return; }
        (await nr.json()).results.forEach(function (r) {
            addLog(r.status === 'success' ? '[OK] ' + r.node : '[FAIL] ' + r.node + ': ' + (r.message||''), r.status === 'success' ? 'success' : 'error');
        });
        addLog('Waiting for nodes...', 'info');
        await waitForReady(sid);
        setStage('aggregate'); addLog('Aggregating...', 'info');
        var ar = await fetch('/analysis/' + sid + '/aggregate', { method: 'POST' });
        if (!ar.ok) { addLog('[FAIL] ' + ar.status, 'error'); setErrorState(); return; }
        var ad = await ar.json();
        if (ad.status === 'completed') {
            setStage('complete'); addLog('[OK] Done!', 'success');
            var s = document.getElementById('analysis-status');
            if (s) { s.className = 'badge bg-success'; s.textContent = 'Completed'; }
            setTimeout(function () { window.location.href = '/analysis/' + sid; }, 2000);
        } else { addLog('[FAIL] ' + (ad.message || ''), 'error'); setErrorState(); }
    } catch (e) { addLog('[ERROR] ' + e, 'error'); setErrorState(); }
}
function waitForReady(sid) {
    return new Promise(function (resolve, reject) {
        (function check() {
            fetch('/api/analysis/' + sid + '/status').then(function (r) { return r.json(); }).then(function (d) {
                if (d.received_nodes !== undefined) {
                    var el = document.getElementById('node-status');
                    if (el) el.innerHTML = '<span class="badge bg-primary">' + d.received_nodes + '/' + d.expected_nodes + '</span>';
                }
                if (d.status === 'ready_for_aggregation' || d.status === 'completed') { addLog('[OK] All submitted', 'success'); resolve(); }
                else setTimeout(check, 2000);
            }).catch(reject);
        })();
    });
}

// ============================================
// Node Testing & Form
// ============================================
function testNode(url) {
    fetch(url + '/health').then(function (r) { return r.json(); })
        .then(function (d) { showToast('OK: ' + d.status, 'success'); })
        .catch(function (e) { showToast('Fail: ' + e, 'danger'); });
}
function initAnalysisForm() {
    var f = document.getElementById('analysisForm'); if (!f) return;
    f.addEventListener('submit', function (e) {
        e.preventDefault();
        var btn = f.querySelector('button[type="submit"]');
        btn.disabled = true; btn.innerHTML = '<span class="spinner-border spinner-border-sm me-2"></span>Starting...';
        fetch(f.getAttribute('data-start-url'), { method: 'POST', body: new FormData(f) })
            .then(function (r) { return r.json(); }).then(function (d) {
                if (d.status === 'success') window.location.href = d.redirect;
                else { showToast(d.message, 'danger'); btn.disabled = false; btn.innerHTML = '<i class="bi bi-play-circle"></i> Start Analysis'; }
            }).catch(function (e) { showToast(e, 'danger'); btn.disabled = false; btn.innerHTML = '<i class="bi bi-play-circle"></i> Start Analysis'; });
    });
}
function initNodesAutoRefresh() { if (document.getElementById('nodes-page')) setTimeout(function () { location.reload(); }, 15000); }

// ============================================
// Analysis Charts (Bar + Radar)
// ============================================
function initAnalysisCharts() {
    var el = document.getElementById('analysis-chart-data'); if (!el) return;
    var cd; try { cd = JSON.parse(el.textContent); } catch (e) { return; }
    var names = cd.algorithms.map(function (a) { return a.name; });
    var sils = cd.algorithms.map(function (a) { return a.avg_silhouette; });
    var cons = cd.algorithms.map(function (a) { return a.consistency; });
    var cols = cd.algorithms.map(function (a) { return a.name === cd.bestAlgorithm ? 'rgb(40,167,69)' : 'rgb(13,110,253)'; });

    var b = document.getElementById('algorithmPerformanceChart');
    if (b && typeof Plotly !== 'undefined')
        Plotly.newPlot(b, [{ x: names, y: sils, type: 'bar', marker: { color: cols }, text: sils.map(function (s) { return s.toFixed(4); }), textposition: 'auto' }],
            { title: 'Silhouette Score by Algorithm', xaxis: { title: 'Algorithm' }, yaxis: { title: 'Score', range: [-0.5, 1] }, height: 400 });

    var r = document.getElementById('ensembleComparisonChart');
    if (r && typeof Plotly !== 'undefined') {
        // Build Davies-Bouldin (inverted, normalized) and Speed scores from sensitivity data
        var sdEl = document.getElementById('sensitivity-data');
        var dbScores = names.map(function () { return 0; });
        var speedScores = names.map(function () { return 0; });

        if (sdEl) {
            try {
                var sd = JSON.parse(sdEl.textContent);
                var matrix = sd.algo_node_matrix || [];

                // Compute avg Davies-Bouldin per algo (lower=better, so invert: 1/(1+db))
                var dbMap = {}, timeMap = {};
                matrix.forEach(function (m) {
                    if (!dbMap[m.algorithm]) { dbMap[m.algorithm] = []; timeMap[m.algorithm] = []; }
                    dbMap[m.algorithm].push(m.davies_bouldin || 0);
                    timeMap[m.algorithm].push(m.execution_time_ms || 1);
                });

                // Find max time for normalization
                var allTimes = matrix.map(function (m) { return m.execution_time_ms || 1; });
                var maxTime = Math.max.apply(null, allTimes) || 1;

                names.forEach(function (name, i) {
                    if (dbMap[name]) {
                        var avgDb = dbMap[name].reduce(function (a, b) { return a + b; }, 0) / dbMap[name].length;
                        dbScores[i] = 1.0 / (1.0 + avgDb); // invert: higher = better
                    }
                    if (timeMap[name]) {
                        var avgTime = timeMap[name].reduce(function (a, b) { return a + b; }, 0) / timeMap[name].length;
                        speedScores[i] = 1.0 - (avgTime / maxTime); // faster = higher
                        speedScores[i] = Math.max(0, Math.min(1, speedScores[i]));
                    }
                });
            } catch (e) { /* fallback: zeros */ }
        }

        Plotly.newPlot(r, [
            { type: 'scatterpolar', r: sils, theta: names, fill: 'toself', name: 'Silhouette', opacity: 0.7 },
            { type: 'scatterpolar', r: dbScores, theta: names, fill: 'toself', name: 'DB Quality (1/(1+DB))', opacity: 0.7 },
            { type: 'scatterpolar', r: speedScores, theta: names, fill: 'toself', name: 'Speed Score', opacity: 0.7 }
        ], {
            polar: { radialaxis: { visible: true, range: [0, 1] } },
            height: 500, showlegend: true
        });
    }
}

// ============================================
// PCA Charts
// ============================================
function initPcaCharts() {
    var el = document.getElementById('pca-chart-data');
    if (!el || typeof Plotly === 'undefined') return;
    var pca; try { pca = JSON.parse(el.textContent); } catch (e) { return; }
    if (!pca.points || !pca.points.length) return;

    var xL = pca.pca_component_labels[0], yL = pca.pca_component_labels[1];

    function buildTraces(groupKey) {
        var groups = {};
        pca.points.forEach(function (p) {
            var k = p[groupKey];
            if (!groups[k]) groups[k] = { x: [], y: [], text: [], sizes: [] };
            groups[k].x.push(p.x); groups[k].y.push(p.y);
            groups[k].text.push(p.label + ' (size:' + p.cluster_size + ')');
            groups[k].sizes.push(Math.max(8, Math.min(30, Math.sqrt(p.cluster_size) * 2)));
        });
        var traces = [];
        Object.keys(groups).sort().forEach(function (k) {
            var g = groups[k];
            traces.push({ x: g.x, y: g.y, text: g.text, mode: 'markers', name: k, type: 'scatter',
                marker: { size: g.sizes, opacity: 0.7 },
                hovertemplate: '%{text}<extra>%{fullData.name}</extra>' });
        });
        return traces;
    }

    var layout = { xaxis: { title: xL }, yaxis: { title: yL }, height: 500, hovermode: 'closest' };

    var e1 = document.getElementById('pcaByNodeChart');
    if (e1) Plotly.newPlot(e1, buildTraces('node_id'), Object.assign({ title: 'By Node' }, layout));
    var e2 = document.getElementById('pcaByAlgorithmChart');
    if (e2) Plotly.newPlot(e2, buildTraces('algorithm'), Object.assign({ title: 'By Algorithm' }, layout));

    // Unified clusters
    var cGroups = {};
    pca.points.forEach(function (p) {
        var k = 'Cluster ' + p.unified_cluster;
        if (!cGroups[k]) cGroups[k] = { x: [], y: [], text: [], sizes: [] };
        cGroups[k].x.push(p.x); cGroups[k].y.push(p.y);
        cGroups[k].text.push(p.label + ' (' + p.node_type + ')');
        cGroups[k].sizes.push(Math.max(8, Math.min(30, Math.sqrt(p.cluster_size) * 2)));
    });
    var cTraces = [];
    Object.keys(cGroups).sort().forEach(function (k) {
        var g = cGroups[k];
        cTraces.push({ x: g.x, y: g.y, text: g.text, mode: 'markers', name: k, type: 'scatter',
            marker: { size: g.sizes, opacity: 0.7 }, hovertemplate: '%{text}<extra>%{fullData.name}</extra>' });
    });
    var e3 = document.getElementById('pcaByClusterChart');
    if (e3) Plotly.newPlot(e3, cTraces, Object.assign({ title: 'Unified Clusters' }, layout));
}

// ============================================
// Sensitivity / Evaluation Charts
// ============================================
function initSensitivityCharts() {
    var el = document.getElementById('sensitivity-data');
    if (!el || typeof Plotly === 'undefined') return;
    var sd; try { sd = JSON.parse(el.textContent); } catch (e) { return; }

    var matrix = sd.algo_node_matrix || [];
    if (!matrix.length) return;

    // --- 1. Quality vs Speed scatter ---
    var qsGroups = {};
    matrix.forEach(function (m) {
        if (!qsGroups[m.algorithm]) qsGroups[m.algorithm] = { x: [], y: [], text: [] };
        qsGroups[m.algorithm].x.push(m.execution_time_ms);
        qsGroups[m.algorithm].y.push(m.silhouette);
        qsGroups[m.algorithm].text.push(m.node_id + ' (' + m.n_clusters + ' clusters)');
    });
    var qsTraces = [];
    Object.keys(qsGroups).forEach(function (algo) {
        var g = qsGroups[algo];
        qsTraces.push({
            x: g.x, y: g.y, text: g.text, mode: 'markers',
            name: algo, type: 'scatter',
            marker: { size: 14, opacity: 0.8 },
            hovertemplate: '%{text}<br>Time: %{x}ms<br>Silhouette: %{y:.4f}<extra>%{fullData.name}</extra>'
        });
    });
    var e1 = document.getElementById('qualitySpeedChart');
    if (e1) Plotly.newPlot(e1, qsTraces, {
        title: 'Quality vs Execution Time', xaxis: { title: 'Execution Time (ms)' },
        yaxis: { title: 'Silhouette Score', range: [-0.5, 1] }, height: 450, hovermode: 'closest'
    });

    // --- 2. Silhouette heatmap (algo x node) ---
    var nodes = [...new Set(matrix.map(function (m) { return m.node_id; }))].sort();
    var algos = [...new Set(matrix.map(function (m) { return m.algorithm; }))].sort();
    var zData = [];
    algos.forEach(function (algo) {
        var row = [];
        nodes.forEach(function (nid) {
            var match = matrix.find(function (m) { return m.algorithm === algo && m.node_id === nid; });
            row.push(match ? match.silhouette : null);
        });
        zData.push(row);
    });
    var e2 = document.getElementById('silhouetteHeatmapChart');
    if (e2) Plotly.newPlot(e2, [{
        z: zData, x: nodes, y: algos, type: 'heatmap',
        colorscale: [[0, '#dc3545'], [0.25, '#ffc107'], [0.5, '#fd7e14'], [0.75, '#20c997'], [1, '#198754']],
        zmin: -0.5, zmax: 1,
        hovertemplate: 'Node: %{x}<br>Algorithm: %{y}<br>Silhouette: %{z:.4f}<extra></extra>'
    }], { title: 'Silhouette Score Heatmap', height: 400, margin: { l: 150 } });

    // --- 3. Clusters found (grouped bar) ---
    var clTraces = [];
    nodes.forEach(function (nid) {
        var nodeData = matrix.filter(function (m) { return m.node_id === nid; });
        clTraces.push({
            x: nodeData.map(function (m) { return m.algorithm; }),
            y: nodeData.map(function (m) { return m.n_clusters; }),
            name: nid, type: 'bar'
        });
    });
    var e3 = document.getElementById('clustersComparisonChart');
    if (e3) Plotly.newPlot(e3, clTraces, {
        title: 'Clusters Found per Algorithm', barmode: 'group',
        xaxis: { title: 'Algorithm' }, yaxis: { title: 'Number of Clusters' }, height: 400
    });

    // --- 4. Execution time (grouped bar) ---
    var etTraces = [];
    nodes.forEach(function (nid) {
        var nodeData = matrix.filter(function (m) { return m.node_id === nid; });
        etTraces.push({
            x: nodeData.map(function (m) { return m.algorithm; }),
            y: nodeData.map(function (m) { return m.execution_time_ms; }),
            name: nid, type: 'bar'
        });
    });
    var e4 = document.getElementById('executionTimeChart');
    if (e4) Plotly.newPlot(e4, etTraces, {
        title: 'Execution Time per Algorithm', barmode: 'group',
        xaxis: { title: 'Algorithm' }, yaxis: { title: 'Time (ms)' }, height: 400
    });
}

// ============================================
// Page Init
// ============================================
document.addEventListener('DOMContentLoaded', function () {
    updateSystemStatus();
    setInterval(updateSystemStatus, 30000);

    [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]')).map(function (el) { return new bootstrap.Tooltip(el); });

    document.querySelectorAll('code').forEach(function (cb) {
        if (cb.textContent.length > 10) {
            var btn = document.createElement('button');
            btn.className = 'btn btn-sm btn-outline-secondary ms-2';
            btn.innerHTML = '<i class="bi bi-clipboard"></i>';
            btn.onclick = function () { copyToClipboard(cb.textContent); };
            cb.parentNode.insertBefore(btn, cb.nextSibling);
        }
    });

    var m = document.getElementById('analysis-monitor-page');
    if (m) { var sid = m.getAttribute('data-session-id'); if (sid) initAnalysisMonitor(sid); }

    initAnalysisForm();
    initNodesAutoRefresh();
    initAnalysisCharts();
    initPcaCharts();
    initSensitivityCharts();
});