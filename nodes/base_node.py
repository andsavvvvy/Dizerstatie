"""
Base Node Class for Distributed Clustering
Every node (medical, retail, IoT) extends this class.
Supports loading datasets from DB (assigned via Dataset Manager).
Auto-seeds default dataset to DB on first run.
"""
from abc import ABC, abstractmethod
import json
import io
import requests
import numpy as np
import pandas as pd
from sklearn.metrics import silhouette_score, davies_bouldin_score
from sklearn.cluster import (
    DBSCAN,
    Birch,
    AgglomerativeClustering,
    SpectralClustering,
    KMeans,
    MiniBatchKMeans,
    MeanShift,
    AffinityPropagation,
)
from sklearn.mixture import GaussianMixture
import logging
import os
import sys
import time
from datetime import datetime
from typing import Dict, Any, Optional

try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False


class BaseNode(ABC):
    """
    Abstract base class for distributed clustering nodes.
    Subclasses must implement load_local_data() and preprocess_data().
    """

    def __init__(self, config_path: str):
        with open(config_path, 'r') as f:
            self.config = json.load(f)

        self.node_id = self.config['node_id']
        self.node_type = self.config['node_type']
        self.port = self.config['port']
        self.algorithms = self.config['algorithms']
        self.global_orchestrator_url = self.config.get('global_orchestrator_url')

        # Flatten algorithm_params into config for backward compat
        algo_params = self.config.get('algorithm_params', {})
        for key, val in algo_params.items():
            if key not in self.config:
                self.config[key] = val

        self.data = None
        self.local_results = {}
        self.system_metrics = {}

        self.logger = self._setup_logger()
        self.logger.info(f"Node initialized: {self.node_id}")

        # Auto-seed default dataset to DB on first run
        self._auto_seed_default_dataset()

    # ========================================
    # Logger
    # ========================================

    def _setup_logger(self) -> logging.Logger:
        logger = logging.getLogger(self.node_id)
        if logger.handlers:
            return logger
        logger.setLevel(logging.INFO)

        os.makedirs('logs', exist_ok=True)
        fh = logging.FileHandler(f'logs/{self.node_id}.log')
        ch = logging.StreamHandler()
        fmt = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(fmt)
        ch.setFormatter(fmt)
        logger.addHandler(fh)
        logger.addHandler(ch)
        return logger

    # ========================================
    # DB Repository helper
    # ========================================

    def _get_repository(self):
        """Lazy import of repository to avoid circular imports."""
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        if project_root not in sys.path:
            sys.path.insert(0, project_root)
        from db.repository import DistributedRepository
        return DistributedRepository

    # ========================================
    # Auto-seed default dataset
    # ========================================

    def _auto_seed_default_dataset(self):
        """
        On first run, if this node has no datasets assigned in DB,
        generate/load local data, upload it to DB as default, and set active.
        """
        try:
            repo = self._get_repository()

            existing = repo.get_node_datasets(self.node_id)
            if existing:
                self.logger.info(
                    f"Node already has {len(existing)} dataset(s) in DB "
                    f"(active: {any(d['is_active'] for d in existing)})"
                )
                return

            self.logger.info("No datasets in DB — seeding default dataset...")

            # Load local data (triggers generation if CSV doesn't exist)
            raw_data = self.load_local_data()

            # Read the CSV file that was just created/loaded
            data_path = self.config.get('data_path', '')
            if os.path.exists(data_path):
                with open(data_path, 'rb') as f:
                    file_bytes = f.read()
                filename = os.path.basename(data_path)
                file_type = 'csv'
            else:
                # No file on disk — create CSV from numpy array
                df = pd.DataFrame(raw_data)
                buf = io.BytesIO()
                df.to_csv(buf, index=False)
                file_bytes = buf.getvalue()
                filename = f'{self.node_id}_default.csv'
                file_type = 'csv'

            # Parse columns info
            df = pd.read_csv(io.BytesIO(file_bytes))
            numeric_cols = list(df.select_dtypes(include='number').columns)
            columns_info = {
                'columns': list(df.columns),
                'dtypes': {col: str(dtype) for col, dtype in df.dtypes.items()},
                'numeric_columns': numeric_cols,
            }

            # Save to DB with is_default=True
            dataset_name = f"{self.node_id} — Default ({self.node_type})"
            dataset_id = repo.save_dataset(
                name=dataset_name,
                original_filename=filename,
                file_type=file_type,
                file_data=file_bytes,
                columns_info=columns_info,
                row_count=len(df),
                description=f"Auto-generated default dataset for {self.node_id}",
                is_default=True,
            )

            # Assign to this node and set as active
            repo.assign_dataset_to_node(self.node_id, dataset_id, set_active=True)

            self.logger.info(
                f"✓ Seeded default dataset to DB: '{dataset_name}' "
                f"(id={dataset_id}, {len(df)} rows, {len(numeric_cols)} numeric cols)"
            )

        except Exception as e:
            self.logger.warning(f"Auto-seed failed (non-fatal, will use local data): {e}")

    # ========================================
    # System Metrics
    # ========================================

    def _collect_system_metrics(self) -> Dict[str, Any]:
        """Collect system-wide CPU and process memory usage."""
        metrics = {
            'cpu_usage_percent': None,
            'memory_usage_mb': None,
        }

        if not HAS_PSUTIL:
            self.logger.warning("psutil not installed — skipping system metrics")
            return metrics

        try:
            # System-wide CPU (0-100%), averaged over a short window
            metrics['cpu_usage_percent'] = round(psutil.cpu_percent(interval=0.5), 1)

            # Process memory in MB
            proc = psutil.Process(os.getpid())
            mem_info = proc.memory_info()
            metrics['memory_usage_mb'] = round(mem_info.rss / (1024 * 1024), 1)

        except Exception as e:
            self.logger.warning(f"Failed to collect system metrics: {e}")

        return metrics

    # ========================================
    # Data Loading
    # ========================================

    def _try_load_from_db(self) -> Optional[np.ndarray]:
        """Try to load active dataset from DB. Returns numpy array or None."""
        try:
            repo = self._get_repository()
            active = repo.get_active_dataset_for_node(self.node_id)
            if not active:
                return None

            self.logger.info(
                f"Loading active dataset from DB: '{active['name']}' "
                f"({active['row_count']} rows, type={active['file_type']})"
            )

            file_data = active['file_data']
            ft = active['file_type']

            if ft == 'csv':
                df = pd.read_csv(io.BytesIO(file_data))
            elif ft in ('xlsx', 'xls'):
                df = pd.read_excel(io.BytesIO(file_data))
            elif ft == 'json':
                df = pd.read_json(io.BytesIO(file_data))
            else:
                self.logger.warning(f"Unknown file type: {ft}")
                return None

            numeric_cols = df.select_dtypes(include=[np.number]).columns
            if len(numeric_cols) == 0:
                self.logger.warning("No numeric columns in dataset")
                return None

            data = df[numeric_cols].values
            self.logger.info(f"Loaded {len(data)} rows x {len(numeric_cols)} cols from DB")
            return data

        except Exception as e:
            self.logger.warning(f"Could not load from DB: {e}")
            return None

    @abstractmethod
    def load_local_data(self) -> np.ndarray:
        """Load data from local file or generate sample data."""
        pass

    @abstractmethod
    def preprocess_data(self, data: np.ndarray) -> np.ndarray:
        """Preprocess data (scaling, imputation, etc.)."""
        pass

    # ========================================
    # Clustering
    # ========================================

    def run_local_clustering(self) -> Dict[str, Any]:
        """Run all configured algorithms on the node's data."""
        self.logger.info(f"Starting local clustering on {self.node_id}")

        # System metrics BEFORE
        metrics_before = self._collect_system_metrics()

        start_time = time.time()

        # Priority: DB dataset > local file > generated data
        self.data = self._try_load_from_db()
        if self.data is None:
            self.logger.info("No active DB dataset — using local data source")
            self.data = self.load_local_data()

        load_time = time.time() - start_time
        self.logger.info(
            f"Loaded {len(self.data)} data points "
            f"with {self.data.shape[1]} features ({load_time:.2f}s)"
        )

        # Preprocess
        self.data = self.preprocess_data(self.data)

        # Run each algorithm
        results = {}
        for algo_name in self.algorithms:
            self.logger.info(f"Running {algo_name}...")
            algo_start = time.time()
            algo_result = self._run_algorithm(algo_name)
            algo_time = int((time.time() - algo_start) * 1000)
            algo_result['execution_time_ms'] = algo_time
            results[algo_name] = algo_result
            self.logger.info(
                f"{algo_name}: {algo_result['n_clusters']} clusters, "
                f"silhouette={algo_result['silhouette']:.3f}, time={algo_time}ms"
            )

        self.local_results = results

        # System metrics AFTER (peak)
        metrics_after = self._collect_system_metrics()
        self.system_metrics = {
            'cpu_usage_percent': max(
                metrics_before.get('cpu_usage_percent') or 0,
                metrics_after.get('cpu_usage_percent') or 0,
            ),
            'memory_usage_mb': max(
                metrics_before.get('memory_usage_mb') or 0,
                metrics_after.get('memory_usage_mb') or 0,
            ),
        }
        self.logger.info(
            f"System metrics — CPU: {self.system_metrics['cpu_usage_percent']}%, "
            f"Memory: {self.system_metrics['memory_usage_mb']}MB"
        )

        return self._build_local_summary()

    def _run_algorithm(self, algo_name: str) -> Dict[str, Any]:
        """Run a specific clustering algorithm and return metrics."""
        n_samples = len(self.data)

        # ---- Algorithm selection ----

        if algo_name == 'KMEANS':
            labels = KMeans(
                n_clusters=self.config.get('kmeans_n_clusters', 3),
                random_state=42,
                n_init=10,
            ).fit_predict(self.data)

        elif algo_name == 'DBSCAN':
            labels = DBSCAN(
                eps=self.config.get('dbscan_eps', 0.5),
                min_samples=self.config.get('dbscan_min_samples', 3),
            ).fit_predict(self.data)

        elif algo_name == 'AGGLO':
            labels = AgglomerativeClustering(
                n_clusters=self.config.get('agglo_n_clusters', 3),
            ).fit_predict(self.data)

        elif algo_name == 'GMM':
            labels = GaussianMixture(
                n_components=self.config.get('gmm_n_components', 3),
                random_state=42,
            ).fit_predict(self.data)

        elif algo_name == 'BIRCH':
            labels = Birch(
                n_clusters=self.config.get('birch_n_clusters', 3),
            ).fit_predict(self.data)

        elif algo_name == 'MEANSHIFT':
            labels = MeanShift(
                bandwidth=self.config.get('meanshift_bandwidth', None),
            ).fit_predict(self.data)

        elif algo_name == 'SPECTRAL':
            n_neighbors = min(
                self.config.get('spectral_n_neighbors', 10),
                n_samples - 1,
            )
            labels = SpectralClustering(
                n_clusters=self.config.get('spectral_n_clusters', 3),
                affinity='nearest_neighbors',
                n_neighbors=n_neighbors,
                random_state=42,
            ).fit_predict(self.data)

        elif algo_name == 'AFFINITY_PROPAGATION':
            labels = AffinityPropagation(
                damping=self.config.get('affinity_damping', 0.7),
                preference=self.config.get('affinity_preference', None),
                max_iter=self.config.get('affinity_max_iter', 300),
                random_state=42,
            ).fit_predict(self.data)

        elif algo_name == 'MINIBATCH_KMEANS':
            labels = MiniBatchKMeans(
                n_clusters=self.config.get('minibatch_n_clusters', 3),
                random_state=42,
                batch_size=self.config.get('minibatch_batch_size', 100),
            ).fit_predict(self.data)

        else:
            raise ValueError(f"Unknown algorithm: {algo_name}")

        # ---- Compute metrics ----

        unique_labels = set(labels)
        if -1 in unique_labels:
            unique_labels.remove(-1)
        n_clusters = len(unique_labels)

        try:
            silhouette = silhouette_score(self.data, labels) if 1 < n_clusters < n_samples else 0.0
        except Exception:
            silhouette = 0.0

        try:
            db_score = davies_bouldin_score(self.data, labels) if n_clusters > 1 else 0.0
        except Exception:
            db_score = 0.0

        # ---- Cluster statistics ----

        centers = []
        sizes = []
        stds = []

        for cid in sorted(unique_labels):
            mask = labels == cid
            cluster_data = self.data[mask]

            if len(cluster_data) > 0:
                centers.append(np.mean(cluster_data, axis=0).tolist())
                sizes.append(int(np.sum(mask)))
                stds.append(np.std(cluster_data, axis=0).tolist())

        return {
            'labels': labels.tolist(),
            'n_clusters': n_clusters,
            'silhouette': float(silhouette),
            'davies_bouldin': float(db_score),
            'cluster_centers': centers,
            'cluster_sizes': sizes,
            'cluster_stds': stds,
            'total_points': n_samples,
        }

    # ========================================
    # Summary & Communication
    # ========================================

    def _build_local_summary(self) -> Dict[str, Any]:
        """Build the summary dict sent to the orchestrator."""
        return {
            'node_id': self.node_id,
            'node_type': self.node_type,
            'timestamp': datetime.now().isoformat(),
            'data_summary': {
                'n_samples': len(self.data),
                'n_features': self.data.shape[1],
                'data_stats': {
                    'mean': np.mean(self.data, axis=0).tolist(),
                    'std': np.std(self.data, axis=0).tolist(),
                    'min': np.min(self.data, axis=0).tolist(),
                    'max': np.max(self.data, axis=0).tolist(),
                },
            },
            'system_metrics': self.system_metrics,
            'results': self.local_results,
        }

    def send_to_global_orchestrator(self, session_id: str) -> Dict[str, Any]:
        """Send clustering results to the global orchestrator."""
        if not self.global_orchestrator_url:
            self.logger.error("No global orchestrator URL configured")
            return {'status': 'error', 'message': 'No orchestrator URL'}

        summary = self._build_local_summary()

        try:
            url = f"{self.global_orchestrator_url}/analysis/{session_id}/receive"
            self.logger.info(f"Sending results to {url}")

            response = requests.post(url, json=summary, timeout=30)
            response.raise_for_status()

            self.logger.info(f"Results sent successfully: {response.status_code}")
            return response.json()

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to send to global orchestrator: {e}")
            return {'status': 'error', 'message': str(e)}

    def get_status(self) -> Dict[str, Any]:
        """Return current node status."""
        return {
            'node_id': self.node_id,
            'node_type': self.node_type,
            'status': 'active',
            'data_loaded': self.data is not None,
            'data_shape': self.data.shape if self.data is not None else None,
            'last_analysis': datetime.now().isoformat() if self.local_results else None,
            'algorithms_available': self.algorithms,
        }