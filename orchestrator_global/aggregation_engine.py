"""
Global Aggregation Engine
Combines results from multiple distributed nodes using meta-clustering
"""
import numpy as np
from scipy.spatial.distance import cdist
from sklearn.cluster import AgglomerativeClustering
from sklearn.decomposition import PCA
from collections import defaultdict
from typing import List, Dict, Any
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class GlobalAggregationEngine:
    """
    Aggregates clustering results from distributed nodes using META-CLUSTERING.
    """

    def __init__(self):
        self.node_results = []

    def receive_node_results(self, node_result: Dict[str, Any]):
        required_fields = ['node_id', 'node_type', 'results', 'data_summary']
        for field in required_fields:
            if field not in node_result:
                raise ValueError(f"Missing required field: {field}")
        self.node_results.append(node_result)
        logger.info(f"Received results from {node_result['node_id']} ({node_result['node_type']})")
        return len(self.node_results)

    def _get_all_algorithms(self) -> set:
        algos = set()
        for nr in self.node_results:
            algos.update(nr['results'].keys())
        return algos

    def _get_node_system_metrics(self) -> Dict[str, Dict]:
        metrics = {}
        for nr in self.node_results:
            nid = nr['node_id']
            sm = nr.get('system_metrics', {})
            if sm and nid not in metrics:
                metrics[nid] = sm
        return metrics

    def _align_centroids(self, all_centroids: List[list]) -> np.ndarray:
        if len(all_centroids) == 0:
            return np.array([])
        max_dim = max(len(c) for c in all_centroids)
        aligned = []
        for c in all_centroids:
            if len(c) < max_dim:
                aligned.append(list(c) + [0.0] * (max_dim - len(c)))
            else:
                aligned.append(list(c))
        return np.array(aligned)

    def _calculate_optimal_threshold(self, centroids: np.ndarray) -> float:
        if len(centroids) < 2:
            return 1.0
        distances = cdist(centroids, centroids)
        non_zero = distances[distances > 0]
        return float(np.median(non_zero)) if len(non_zero) > 0 else 1.0

    def _calculate_cohesion(self, centroids: np.ndarray, global_centroid: np.ndarray) -> float:
        if len(centroids) == 1:
            return 1.0
        distances = np.linalg.norm(centroids - global_centroid, axis=1)
        return float(1.0 / (1.0 + np.mean(distances)))

    def _interpret_global_cluster(self, org_types, total_points, cohesion):
        if len(org_types) == 1:
            return (f"Organization-specific pattern ({org_types[0]}). "
                    f"Contains {total_points} data points from local analysis.")
        interpretation = f"Cross-organizational pattern detected across {', '.join(org_types)}. "
        if cohesion > 0.7:
            interpretation += "High cohesion suggests strong correlation between organizations."
        elif cohesion > 0.4:
            interpretation += "Moderate cohesion indicates some shared characteristics."
        else:
            interpretation += "Low cohesion suggests diverse local patterns."
        if 'healthcare' in org_types and 'retail' in org_types:
            interpretation += " Potential correlation between health metrics and consumer behavior."
        if 'iot' in org_types and 'healthcare' in org_types:
            interpretation += " Environmental factors may influence health outcomes."
        if 'retail' in org_types and 'iot' in org_types:
            interpretation += " Consumer behavior correlated with environmental conditions."
        return interpretation

    def aggregate_by_algorithm(self, algorithm: str) -> Dict[str, Any]:
        logger.info(f"\n{'='*60}\nAggregating: {algorithm}\n{'='*60}")

        all_centroids, all_weights, centroid_metadata = [], [], []

        for nr in self.node_results:
            if algorithm not in nr['results']:
                logger.warning(f"Algorithm {algorithm} not found in {nr['node_id']}")
                continue
            ar = nr['results'][algorithm]
            for i, (center, size) in enumerate(zip(ar['cluster_centers'], ar['cluster_sizes'])):
                all_centroids.append(center)
                all_weights.append(size)
                centroid_metadata.append({
                    'node_id': nr['node_id'], 'node_type': nr['node_type'],
                    'local_cluster_id': i, 'local_silhouette': ar['silhouette'],
                })

        if not all_centroids:
            return {'error': f'No results for {algorithm}', 'algorithm': algorithm}

        all_centroids = self._align_centroids(all_centroids)
        all_weights = np.array(all_weights)
        logger.info(f"Total local clusters: {len(all_centroids)}")

        if len(all_centroids) < 2:
            labels = np.array([0])
        else:
            threshold = self._calculate_optimal_threshold(all_centroids)
            logger.info(f"Distance threshold: {threshold:.3f}")
            labels = AgglomerativeClustering(
                n_clusters=None, distance_threshold=threshold, linkage='average'
            ).fit_predict(all_centroids)

        n_global = len(set(labels))
        logger.info(f"Merged {len(all_centroids)} -> {n_global} global clusters")

        clusters = defaultdict(list)
        for idx, lbl in enumerate(labels):
            clusters[lbl].append({
                'centroid': all_centroids[idx], 'weight': all_weights[idx],
                'source_node': centroid_metadata[idx]['node_id'],
                'node_type': centroid_metadata[idx]['node_type'],
                'local_cluster': centroid_metadata[idx]['local_cluster_id'],
                'local_silhouette': centroid_metadata[idx]['local_silhouette'],
            })

        summary = {}
        for cid, members in clusters.items():
            centroids = np.array([m['centroid'] for m in members])
            weights = np.array([m['weight'] for m in members])
            gc = np.average(centroids, axis=0, weights=weights)
            tp = int(np.sum(weights))
            orgs = set(m['node_type'] for m in members)
            coh = self._calculate_cohesion(centroids, gc)

            summary[f"global_cluster_{cid}"] = {
                'centroid': gc.tolist(), 'total_points': tp,
                'n_local_clusters_merged': len(members),
                'contributing_nodes': [m['source_node'] for m in members],
                'contributing_org_types': list(orgs),
                'is_cross_organizational': len(orgs) > 1,
                'avg_local_silhouette': float(np.mean([m['local_silhouette'] for m in members])),
                'cohesion': coh,
                'interpretation': self._interpret_global_cluster(list(orgs), tp, coh),
            }
            logger.info(f"  Cluster {cid}: {tp} pts, {len(orgs)} orgs, cohesion={coh:.3f}")

        return {
            'algorithm': algorithm, 'n_global_clusters': n_global,
            'clusters': summary, 'total_points_represented': int(np.sum(all_weights)),
        }

    def ensemble_across_algorithms(self) -> Dict[str, Any]:
        logger.info(f"\n{'='*60}\nENSEMBLE ANALYSIS\n{'='*60}")

        algo_scores = defaultdict(lambda: {
            'silhouettes': [], 'davies_bouldins': [], 'n_clusters': [], 'nodes': []
        })

        for nr in self.node_results:
            for algo, res in nr['results'].items():
                algo_scores[algo]['silhouettes'].append(res['silhouette'])
                algo_scores[algo]['davies_bouldins'].append(res.get('davies_bouldin', 0))
                algo_scores[algo]['n_clusters'].append(res['n_clusters'])
                algo_scores[algo]['nodes'].append(nr['node_id'])

        summary = {}
        for algo, sc in algo_scores.items():
            sils = np.array(sc['silhouettes'])
            cons = 1.0 / (1.0 + np.std(sils))
            summary[algo] = {
                'avg_silhouette': float(np.mean(sils)),
                'std_silhouette': float(np.std(sils)),
                'min_silhouette': float(np.min(sils)),
                'max_silhouette': float(np.max(sils)),
                'avg_clusters': float(np.mean(sc['n_clusters'])),
                'consistency': float(cons),
                'nodes_count': len(sc['nodes']),
                'nodes_list': sc['nodes'],
            }
            logger.info(f"{algo}: avg_sil={summary[algo]['avg_silhouette']:.3f}, consistency={cons:.3f}")

        best = max(summary.keys(),
                   key=lambda a: summary[a]['avg_silhouette'] * 0.7 + summary[a]['consistency'] * 0.3)
        logger.info(f"\nBest algorithm: {best}")

        return {
            'best_algorithm': best, 'algorithm_scores': summary,
            'recommendation': self._generate_recommendation(summary, best),
        }

    def _generate_recommendation(self, scores, best):
        bs = scores[best]
        q = ("excellent" if bs['avg_silhouette'] > 0.7 else
             "good" if bs['avg_silhouette'] > 0.5 else
             "moderate" if bs['avg_silhouette'] > 0.3 else "poor")
        c = ("highly consistent" if bs['consistency'] > 0.8 else
             "moderately consistent" if bs['consistency'] > 0.6 else "variable")
        rec = (f"{best} shows {q} clustering performance "
               f"(avg silhouette: {bs['avg_silhouette']:.3f}). "
               f"The algorithm is {c} across {bs['nodes_count']} nodes. ")
        rec += ("Recommended for production."
                if q in ("excellent", "good")
                else "Consider parameter tuning.")
        return rec

    def extract_cross_org_insights(self) -> Dict[str, Any]:
        """
        Detect cross-organizational patterns using UNIFIED meta-clustering.
        Collects ALL centroids from ALL algorithms from ALL nodes.
        """
        logger.info(f"\n{'='*60}\nCROSS-ORGANIZATIONAL INSIGHTS (UNIFIED)\n{'='*60}")

        insights = {
            'cross_org_clusters': [],
            'org_specific_patterns': [],
            'summary_stats': {},
        }

        all_centroids, all_weights, centroid_meta = [], [], []

        for nr in self.node_results:
            for algo, ar in nr['results'].items():
                for i, (center, size) in enumerate(zip(ar['cluster_centers'], ar['cluster_sizes'])):
                    all_centroids.append(center)
                    all_weights.append(size)
                    centroid_meta.append({
                        'node_id': nr['node_id'],
                        'node_type': nr['node_type'],
                        'algorithm': algo,
                        'local_cluster_id': i,
                        'silhouette': ar['silhouette'],
                    })

        if len(all_centroids) < 2:
            logger.info("Not enough centroids for unified analysis")
            return insights

        all_centroids = self._align_centroids(all_centroids)
        all_weights = np.array(all_weights)

        logger.info(f"Unified: {len(all_centroids)} centroids, "
                     f"{len(self.node_results)} nodes, "
                     f"{len(self._get_all_algorithms())} algorithms")

        base_threshold = self._calculate_optimal_threshold(all_centroids)
        threshold = base_threshold * 1.2
        logger.info(f"Threshold: {threshold:.3f} (base: {base_threshold:.3f})")

        labels = AgglomerativeClustering(
            n_clusters=None, distance_threshold=threshold, linkage='average'
        ).fit_predict(all_centroids)

        clusters = defaultdict(list)
        for idx, lbl in enumerate(labels):
            clusters[lbl].append({
                'centroid': all_centroids[idx],
                'weight': all_weights[idx],
                **centroid_meta[idx],
            })

        cross_org_count = 0
        org_specific_count = 0
        node_samples = {}
        for nr in self.node_results:
            node_samples[nr['node_id']] = nr['data_summary']['n_samples']

        for cid, members in clusters.items():
            org_types = set(m['node_type'] for m in members)
            node_ids = set(m['node_id'] for m in members)
            algorithms = set(m['algorithm'] for m in members)
            centroids = np.array([m['centroid'] for m in members])
            weights = np.array([m['weight'] for m in members])

            gc = np.average(centroids, axis=0, weights=weights)
            total_points = int(np.sum(weights))
            cohesion = self._calculate_cohesion(centroids, gc)

            if len(org_types) > 1:
                cross_org_count += 1
                unique_points = sum(node_samples.get(nid, 0) for nid in node_ids)

                insights['cross_org_clusters'].append({
                    'cluster_id': f'unified_cluster_{cid}',
                    'organizations': sorted(list(org_types)),
                    'nodes': sorted(list(node_ids)),
                    'algorithms': sorted(list(algorithms)),
                    'size': total_points,
                    'unique_data_points': unique_points,
                    'n_local_clusters': len(members),
                    'cohesion': cohesion,
                    'interpretation': self._interpret_cross_org_insight(
                        list(org_types), list(algorithms), unique_points, cohesion
                    ),
                })
                logger.info(f"✓ Cross-org: {', '.join(org_types)} | "
                             f"{len(algorithms)} algos | {unique_points} unique pts | "
                             f"cohesion={cohesion:.3f}")
            else:
                org_specific_count += 1
                insights['org_specific_patterns'].append({
                    'organization': list(org_types)[0],
                    'algorithms': sorted(list(algorithms)),
                    'size': total_points,
                    'cohesion': cohesion,
                })

        insights['summary_stats'] = {
            'total_unified_clusters': len(clusters),
            'cross_org_clusters': cross_org_count,
            'org_specific_clusters': org_specific_count,
            'total_centroids_analyzed': len(all_centroids),
            'threshold_used': float(threshold),
        }

        logger.info(f"\nResults: {len(clusters)} clusters, "
                     f"{cross_org_count} cross-org, {org_specific_count} org-specific")

        return insights

    def _interpret_cross_org_insight(self, org_types, algorithms, total_points, cohesion):
        interpretation = (
            f"Unified pattern spanning {', '.join(org_types)} organizations, "
            f"detected across {len(algorithms)} algorithm(s) ({', '.join(algorithms)}). "
            f"Involves {total_points} data points. "
        )
        if cohesion > 0.7:
            interpretation += "Very high cohesion — strong structural similarity. "
        elif cohesion > 0.4:
            interpretation += "Moderate cohesion — shared characteristics detected. "
        else:
            interpretation += "Low cohesion — loose similarity. "

        orgs = set(org_types)
        if 'healthcare' in orgs and 'retail' in orgs:
            interpretation += "Health demographics correlate with consumer purchasing. "
        if 'iot' in orgs and 'healthcare' in orgs:
            interpretation += "Environmental factors may impact patient outcomes. "
        if 'retail' in orgs and 'iot' in orgs:
            interpretation += "Consumer behavior aligns with environmental conditions. "
        if len(orgs) == 3:
            interpretation += "GLOBAL pattern spanning all domains — significant finding. "
        return interpretation

    def generate_pca_visualization(self) -> Dict[str, Any]:
        """
        Generate 2D PCA projections of all centroids for visualization.
        Returns data structured for Plotly scatter plots:
        - Per-node view (color by node)
        - Per-algorithm view (color by algorithm)
        - Global clusters view (color by unified cluster label)
        """
        logger.info("Generating PCA visualization data...")

        all_centroids, centroid_meta = [], []

        for nr in self.node_results:
            for algo, ar in nr['results'].items():
                for i, (center, size) in enumerate(zip(ar['cluster_centers'], ar['cluster_sizes'])):
                    all_centroids.append(center)
                    centroid_meta.append({
                        'node_id': nr['node_id'],
                        'node_type': nr['node_type'],
                        'algorithm': algo,
                        'cluster_id': i,
                        'cluster_size': size,
                        'silhouette': ar['silhouette'],
                    })

        if len(all_centroids) < 3:
            logger.warning("Not enough centroids for PCA (need >= 3)")
            return {}

        aligned = self._align_centroids(all_centroids)
        pca = PCA(n_components=2)
        coords_2d = pca.fit_transform(aligned)

        explained_variance = pca.explained_variance_ratio_.tolist()
        threshold = self._calculate_optimal_threshold(aligned) * 1.2
        if len(aligned) >= 2:
            unified_labels = AgglomerativeClustering(
                n_clusters=None, distance_threshold=threshold, linkage='average'
            ).fit_predict(aligned)
        else:
            unified_labels = np.array([0] * len(aligned))
        points = []
        for i, meta in enumerate(centroid_meta):
            points.append({
                'x': float(coords_2d[i, 0]),
                'y': float(coords_2d[i, 1]),
                'node_id': meta['node_id'],
                'node_type': meta['node_type'],
                'algorithm': meta['algorithm'],
                'cluster_id': meta['cluster_id'],
                'cluster_size': meta['cluster_size'],
                'silhouette': meta['silhouette'],
                'unified_cluster': int(unified_labels[i]),
                'label': f"{meta['node_id']}:{meta['algorithm']}:C{meta['cluster_id']}",
            })

        viz_data = {
            'points': points,
            'explained_variance': explained_variance,
            'total_variance_explained': float(sum(explained_variance)),
            'n_centroids': len(points),
            'pca_component_labels': [
                f"PC1 ({explained_variance[0]*100:.1f}%)",
                f"PC2 ({explained_variance[1]*100:.1f}%)",
            ],
        }

        logger.info(f"PCA visualization: {len(points)} points, "
                     f"variance explained: {viz_data['total_variance_explained']*100:.1f}%")

        return viz_data

    def generate_global_report(self) -> Dict[str, Any]:
        logger.info(f"\n{'='*60}\nGENERATING GLOBAL REPORT\n{'='*60}")

        total_points = sum(r['data_summary']['n_samples'] for r in self.node_results)
        node_system_metrics = self._get_node_system_metrics()

        report = {
            'metadata': {
                'total_nodes': len(self.node_results),
                'total_data_points': total_points,
                'node_system_metrics': node_system_metrics,
                'nodes_summary': [
                    {
                        'node_id': r['node_id'], 'node_type': r['node_type'],
                        'n_samples': r['data_summary']['n_samples'],
                        'n_features': r['data_summary']['n_features'],
                        'algorithms_used': list(r['results'].keys()),
                    }
                    for r in self.node_results
                ],
            },
        }
        all_algos = self._get_all_algorithms()
        report['algorithm_aggregations'] = {}
        for algo in sorted(all_algos):
            report['algorithm_aggregations'][algo] = self.aggregate_by_algorithm(algo)
        report['ensemble_analysis'] = self.ensemble_across_algorithms()
        report['insights'] = self.extract_cross_org_insights()
        report['pca_visualization'] = self.generate_pca_visualization()

        logger.info(f"\nReport generated:")
        logger.info(f"  Data points: {total_points:,}")
        logger.info(f"  Algorithms: {len(all_algos)}")
        logger.info(f"  Best: {report['ensemble_analysis']['best_algorithm']}")
        logger.info(f"  Cross-org patterns: {len(report['insights']['cross_org_clusters'])}")
        logger.info(f"  PCA points: {len(report.get('pca_visualization', {}).get('points', []))}")

        return report