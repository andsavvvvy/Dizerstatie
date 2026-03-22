"""
Database repository for distributed clustering system
"""
from db.connection import get_connection
from typing import Dict, List, Any, Optional
import json
import logging

logger = logging.getLogger(__name__)


class DistributedRepository:

    # ---- Node Management ----

    @staticmethod
    def register_node(node_config):
        conn = get_connection(); cur = conn.cursor()
        try:
            cur.execute("""
                INSERT INTO distributed_nodes (node_id, node_type, location, config_json, status)
                VALUES (%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE config_json=VALUES(config_json), status=VALUES(status),
                    updated_at=CURRENT_TIMESTAMP
            """, (node_config['node_id'], node_config['node_type'],
                  node_config.get('location','localhost'), json.dumps(node_config), 'active'))
            conn.commit(); return cur.lastrowid
        except: conn.rollback(); raise
        finally: cur.close(); conn.close()

    @staticmethod
    def update_node_heartbeat(node_id):
        conn = get_connection(); cur = conn.cursor()
        try:
            cur.execute("UPDATE distributed_nodes SET last_heartbeat=CURRENT_TIMESTAMP WHERE node_id=%s", (node_id,))
            conn.commit()
        except Exception as e: logger.error(f"Heartbeat fail: {e}")
        finally: cur.close(); conn.close()

    @staticmethod
    def get_all_nodes():
        conn = get_connection(); cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SELECT node_id, node_type, location, status, last_heartbeat, total_analyses, total_data_points_processed, created_at FROM distributed_nodes ORDER BY created_at DESC")
            return cur.fetchall()
        finally: cur.close(); conn.close()

    # ---- Local Results ----

    @staticmethod
    def save_local_results(session_id, node_id, results):
        conn = get_connection(); cur = conn.cursor()
        try:
            for algo, ar in results.items():
                cur.execute("""
                    INSERT INTO node_local_results (node_id, session_id, algorithm, n_local_clusters,
                        silhouette_score, davies_bouldin_score, execution_time_ms,
                        cluster_centers, cluster_sizes, cluster_stds, data_summary)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (node_id, session_id, algo, ar['n_clusters'], ar['silhouette'],
                      ar.get('davies_bouldin'), ar.get('execution_time_ms',0),
                      json.dumps(ar['cluster_centers']), json.dumps(ar['cluster_sizes']),
                      json.dumps(ar.get('cluster_stds',[])), json.dumps(ar.get('data_summary',{}))))
            conn.commit(); logger.info(f"✓ Saved local results: {node_id}/{session_id}")
        except: conn.rollback(); raise
        finally: cur.close(); conn.close()

    # ---- Global Analysis ----

    @staticmethod
    def create_global_analysis(session_id, total_nodes, total_points):
        conn = get_connection(); cur = conn.cursor()
        try:
            cur.execute("INSERT INTO global_analyses (session_id, total_nodes, total_data_points, status) VALUES (%s,%s,%s,%s)",
                        (session_id, total_nodes, total_points, 'processing'))
            conn.commit(); return cur.lastrowid
        except: conn.rollback(); raise
        finally: cur.close(); conn.close()

    @staticmethod
    def complete_global_analysis(session_id, report, execution_time_ms, node_system_metrics=None):
        conn = get_connection(); cur = conn.cursor()
        try:
            ens = report['ensemble_analysis']
            meta = report.get('metadata', {})
            tdp = meta.get('total_data_points', 0)
            tn = meta.get('total_nodes', 0)
            nsm = node_system_metrics or {}

            cur.execute("""
                UPDATE global_analyses SET total_nodes=%s, total_data_points=%s, best_algorithm=%s,
                    best_algorithm_score=%s, global_clusters=%s, algorithm_aggregations=%s,
                    ensemble_analysis=%s, cross_org_insights=%s, pca_visualization=%s,
                    execution_time_total_ms=%s,
                    status=%s, completed_at=CURRENT_TIMESTAMP WHERE session_id=%s
            """, (tn, tdp, ens['best_algorithm'],
                  ens['algorithm_scores'][ens['best_algorithm']]['avg_silhouette'],
                  json.dumps(report.get('global_clusters',{})),
                  json.dumps(report.get('algorithm_aggregations',{})),
                  json.dumps(ens), json.dumps(report.get('insights',{})),
                  json.dumps(report.get('pca_visualization',{})),
                  execution_time_ms, 'completed', session_id))

            cur.execute("SELECT id FROM global_analyses WHERE session_id=%s", (session_id,))
            ga_id = cur.fetchone()
            ga_id = ga_id[0] if ga_id else None

            if ga_id and meta.get('nodes_summary'):
                for ni in meta['nodes_summary']:
                    nid, ns = ni['node_id'], ni.get('n_samples',0)
                    bla, bls = None, -1.0
                    for an, ad in ens.get('algorithm_scores',{}).items():
                        if nid in ad.get('nodes_list',[]) and ad['avg_silhouette'] > bls:
                            bls, bla = ad['avg_silhouette'], an
                    cur.execute("""
                        INSERT INTO analysis_node_participation (global_analysis_id, node_id,
                            contribution_weight, data_points_contributed, best_local_algorithm, best_local_score)
                        VALUES (%s,%s,%s,%s,%s,%s)
                    """, (ga_id, nid, round(ns/tdp if tdp else 0,4), ns, bla,
                          round(bls,4) if bls>=0 else None))

            if meta.get('nodes_summary'):
                for ni in meta['nodes_summary']:
                    nid = ni['node_id']
                    cur.execute("SELECT AVG(silhouette_score), COUNT(*), AVG(execution_time_ms) FROM node_local_results WHERE node_id=%s AND session_id=%s", (nid, session_id))
                    pr = cur.fetchone()
                    sm = nsm.get(nid, {})
                    if pr and pr[0] is not None:
                        cur.execute("""
                            INSERT INTO node_performance_metrics (node_id, avg_silhouette_7d,
                                total_analyses_7d, avg_execution_time_ms, cpu_usage_percent, memory_usage_mb)
                            VALUES (%s,%s,%s,%s,%s,%s)
                        """, (nid, round(float(pr[0]),4), int(pr[1]),
                              int(pr[2]) if pr[2] else 0,
                              sm.get('cpu_usage_percent'), sm.get('memory_usage_mb')))
                for ni in meta['nodes_summary']:
                    cur.execute("UPDATE distributed_nodes SET total_analyses=total_analyses+1, total_data_points_processed=total_data_points_processed+%s WHERE node_id=%s",
                                (ni.get('n_samples',0), ni['node_id']))

            conn.commit(); logger.info(f"✓ Completed global analysis: {session_id}")
        except: conn.rollback(); raise
        finally: cur.close(); conn.close()

    @staticmethod
    def get_global_analysis(session_id):
        conn = get_connection(); cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SELECT * FROM global_analyses WHERE session_id=%s", (session_id,))
            row = cur.fetchone()
            if row:
                for f in ('global_clusters','algorithm_aggregations','ensemble_analysis','cross_org_insights','pca_visualization'):
                    if row.get(f) and isinstance(row[f], str): row[f] = json.loads(row[f])
            return row
        finally: cur.close(); conn.close()

    @staticmethod
    def list_recent_analyses(limit=10):
        conn = get_connection(); cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SELECT id, session_id, total_nodes, total_data_points, best_algorithm, best_algorithm_score, status, created_at, completed_at, execution_time_total_ms FROM global_analyses ORDER BY created_at DESC LIMIT %s", (limit,))
            return cur.fetchall()
        finally: cur.close(); conn.close()

    @staticmethod
    def delete_analysis(session_id):
        """Delete analysis + all related data."""
        conn = get_connection(); cur = conn.cursor()
        try:
            cur.execute("SELECT id FROM global_analyses WHERE session_id=%s", (session_id,))
            row = cur.fetchone()
            cur.execute("DELETE FROM node_local_results WHERE session_id=%s", (session_id,))
            if row:
                cur.execute("DELETE FROM analysis_node_participation WHERE global_analysis_id=%s", (row[0],))
            cur.execute("DELETE FROM global_analyses WHERE session_id=%s", (session_id,))
            conn.commit(); logger.info(f"✓ Deleted analysis: {session_id}")
        except: conn.rollback(); raise
        finally: cur.close(); conn.close()

    # ---- Participation & Performance ----

    @staticmethod
    def get_node_participation(session_id):
        conn = get_connection(); cur = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT anp.node_id, anp.contribution_weight, anp.data_points_contributed,
                    anp.best_local_algorithm, anp.best_local_score, dn.node_type, dn.location
                FROM analysis_node_participation anp
                JOIN global_analyses ga ON ga.id=anp.global_analysis_id
                LEFT JOIN distributed_nodes dn ON dn.node_id=anp.node_id
                WHERE ga.session_id=%s ORDER BY anp.data_points_contributed DESC
            """, (session_id,))
            return cur.fetchall()
        finally: cur.close(); conn.close()

    @staticmethod
    def get_node_performance(session_id):
        conn = get_connection(); cur = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT npm.* FROM node_performance_metrics npm
                WHERE npm.recorded_at >= (SELECT created_at FROM global_analyses WHERE session_id=%s)
                  AND npm.recorded_at <= (SELECT COALESCE(completed_at, NOW()) FROM global_analyses WHERE session_id=%s)
                ORDER BY npm.node_id
            """, (session_id, session_id))
            return cur.fetchall()
        finally: cur.close(); conn.close()

    @staticmethod
    def get_local_results_for_session(session_id):
        conn = get_connection(); cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SELECT node_id, algorithm, n_local_clusters, silhouette_score, davies_bouldin_score, execution_time_ms FROM node_local_results WHERE session_id=%s ORDER BY node_id, algorithm", (session_id,))
            return cur.fetchall()
        finally: cur.close(); conn.close()

    # ---- Dataset Management ----

    @staticmethod
    def save_dataset(name, original_filename, file_type, file_data, columns_info, row_count, description='', is_default=False):
        conn = get_connection(); cur = conn.cursor()
        try:
            cur.execute("""
                INSERT INTO datasets (name, original_filename, file_type, file_data, columns_info,
                    row_count, file_size_bytes, is_default, description)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (name, original_filename, file_type, file_data, json.dumps(columns_info),
                  row_count, len(file_data), 1 if is_default else 0, description))
            conn.commit(); did = cur.lastrowid
            logger.info(f"✓ Saved dataset: {name} ({row_count} rows)")
            return did
        except: conn.rollback(); raise
        finally: cur.close(); conn.close()

    @staticmethod
    def list_datasets():
        conn = get_connection(); cur = conn.cursor(dictionary=True)
        try:
            cur.execute("SELECT id, name, original_filename, file_type, columns_info, row_count, file_size_bytes, is_default, description, uploaded_at FROM datasets ORDER BY is_default DESC, uploaded_at DESC")
            rows = cur.fetchall()
            for r in rows:
                if r.get('columns_info') and isinstance(r['columns_info'], str):
                    r['columns_info'] = json.loads(r['columns_info'])
            return rows
        finally: cur.close(); conn.close()

    @staticmethod
    def delete_dataset(dataset_id):
        """Delete a user-uploaded dataset. Default datasets cannot be deleted."""
        conn = get_connection(); cur = conn.cursor()
        try:
            cur.execute("SELECT is_default FROM datasets WHERE id=%s", (dataset_id,))
            row = cur.fetchone()
            if not row: raise ValueError("Dataset not found")
            if row[0] == 1: raise ValueError("Cannot delete default datasets")
            cur.execute("DELETE FROM datasets WHERE id=%s AND is_default=0", (dataset_id,))
            conn.commit()
        except: conn.rollback(); raise
        finally: cur.close(); conn.close()

    @staticmethod
    def get_dataset_file(dataset_id):
        conn = get_connection(); cur = conn.cursor()
        try:
            cur.execute("SELECT file_data, original_filename, file_type FROM datasets WHERE id=%s", (dataset_id,))
            return cur.fetchone()
        finally: cur.close(); conn.close()

    # ---- Node-Dataset Assignments ----

    @staticmethod
    def assign_dataset_to_node(node_id, dataset_id, set_active=False):
        conn = get_connection(); cur = conn.cursor()
        try:
            if set_active:
                cur.execute("UPDATE node_dataset_assignments SET is_active=0 WHERE node_id=%s", (node_id,))
            cur.execute("""
                INSERT INTO node_dataset_assignments (node_id, dataset_id, is_active)
                VALUES (%s,%s,%s)
                ON DUPLICATE KEY UPDATE is_active=VALUES(is_active), assigned_at=CURRENT_TIMESTAMP
            """, (node_id, dataset_id, 1 if set_active else 0))
            conn.commit()
        except: conn.rollback(); raise
        finally: cur.close(); conn.close()

    @staticmethod
    def set_active_dataset(node_id, dataset_id):
        conn = get_connection(); cur = conn.cursor()
        try:
            cur.execute("UPDATE node_dataset_assignments SET is_active=0 WHERE node_id=%s", (node_id,))
            cur.execute("UPDATE node_dataset_assignments SET is_active=1 WHERE node_id=%s AND dataset_id=%s", (node_id, dataset_id))
            conn.commit()
        except: conn.rollback(); raise
        finally: cur.close(); conn.close()

    @staticmethod
    def unassign_dataset(node_id, dataset_id):
        conn = get_connection(); cur = conn.cursor()
        try:
            cur.execute("DELETE FROM node_dataset_assignments WHERE node_id=%s AND dataset_id=%s", (node_id, dataset_id))
            conn.commit()
        except: conn.rollback(); raise
        finally: cur.close(); conn.close()

    @staticmethod
    def get_node_datasets(node_id):
        conn = get_connection(); cur = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT d.id, d.name, d.original_filename, d.file_type, d.row_count,
                    d.file_size_bytes, d.columns_info, d.is_default, nda.is_active, nda.assigned_at
                FROM node_dataset_assignments nda
                JOIN datasets d ON d.id=nda.dataset_id
                WHERE nda.node_id=%s ORDER BY nda.is_active DESC, nda.assigned_at DESC
            """, (node_id,))
            rows = cur.fetchall()
            for r in rows:
                if r.get('columns_info') and isinstance(r['columns_info'], str):
                    r['columns_info'] = json.loads(r['columns_info'])
            return rows
        finally: cur.close(); conn.close()

    @staticmethod
    def get_active_dataset_for_node(node_id):
        conn = get_connection(); cur = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT d.id, d.name, d.file_type, d.file_data, d.columns_info, d.row_count
                FROM node_dataset_assignments nda
                JOIN datasets d ON d.id=nda.dataset_id
                WHERE nda.node_id=%s AND nda.is_active=1 LIMIT 1
            """, (node_id,))
            row = cur.fetchone()
            if row and row.get('columns_info') and isinstance(row['columns_info'], str):
                row['columns_info'] = json.loads(row['columns_info'])
            return row
        finally: cur.close(); conn.close()

    @staticmethod
    def get_all_node_assignments():
        conn = get_connection(); cur = conn.cursor(dictionary=True)
        try:
            cur.execute("""
                SELECT nda.node_id, nda.dataset_id, nda.is_active,
                    d.name AS dataset_name, d.row_count, d.file_type, d.is_default, dn.node_type
                FROM node_dataset_assignments nda
                JOIN datasets d ON d.id=nda.dataset_id
                LEFT JOIN distributed_nodes dn ON dn.node_id=nda.node_id
                ORDER BY nda.node_id, nda.is_active DESC
            """)
            return cur.fetchall()
        finally: cur.close(); conn.close()