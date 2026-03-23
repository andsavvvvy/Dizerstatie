import numpy as np
from scipy.spatial.distance import cdist, cosine
from sklearn.cluster import AgglomerativeClustering
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
from collections import defaultdict
from typing import List, Dict, Any
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class GlobalAggregationEngine:

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

        best = max(summary.keys(),
                   key=lambda a: summary[a]['avg_silhouette'] * 0.7 + summary[a]['consistency'] * 0.3)

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

    def _compute_multi_metric_distances(self, centroids: np.ndarray) -> Dict[str, np.ndarray]:
        euclidean = cdist(centroids, centroids, metric='euclidean')

        norms = np.linalg.norm(centroids, axis=1, keepdims=True)
        norms[norms == 0] = 1e-10
        normalized = centroids / norms
        cosine_dist = cdist(normalized, normalized, metric='cosine')
        cosine_dist = np.nan_to_num(cosine_dist, nan=1.0)

        n = len(centroids)
        corr_dist = np.zeros((n, n))
        for i in range(n):
            for j in range(i + 1, n):
                if np.std(centroids[i]) > 0 and np.std(centroids[j]) > 0:
                    corr = np.corrcoef(centroids[i], centroids[j])[0, 1]
                    corr_dist[i, j] = 1.0 - abs(corr)
                else:
                    corr_dist[i, j] = 1.0
                corr_dist[j, i] = corr_dist[i, j]

        return {
            'euclidean': euclidean,
            'cosine': cosine_dist,
            'correlation': corr_dist,
        }

    def _compute_combined_distance(self, centroids: np.ndarray,
                                    weights: Dict[str, float] = None) -> np.ndarray:
        if weights is None:
            weights = {'euclidean': 0.5, 'cosine': 0.25, 'correlation': 0.25}

        metrics = self._compute_multi_metric_distances(centroids)

        normalized_metrics = {}
        for name, dist_matrix in metrics.items():
            max_val = np.max(dist_matrix)
            if max_val > 0:
                normalized_metrics[name] = dist_matrix / max_val
            else:
                normalized_metrics[name] = dist_matrix

        combined = np.zeros_like(list(normalized_metrics.values())[0])
        for name, dist_matrix in normalized_metrics.items():
            combined += weights.get(name, 0) * dist_matrix

        return combined

    def _analyze_feature_contributions(self, members: list, n_features: int) -> Dict[str, Any]:
        centroids = np.array([m['centroid'] for m in members])

        if len(centroids) < 2:
            return {'top_features': [], 'feature_distances': []}

        node_groups = defaultdict(list)
        for m in members:
            node_groups[m['node_type']].append(m['centroid'])

        org_types = list(node_groups.keys())
        if len(org_types) < 2:
            return {'top_features': [], 'feature_distances': []}

        org_means = {}
        for org, cents in node_groups.items():
            org_means[org] = np.mean(cents, axis=0)

        feature_similarities = []
        for dim in range(n_features):
            values = [org_means[org][dim] for org in org_types]
            spread = max(values) - min(values)
            overall_range = np.max(centroids[:, dim]) - np.min(centroids[:, dim])
            if overall_range > 0:
                similarity = 1.0 - (spread / overall_range)
            else:
                similarity = 1.0
            feature_similarities.append({
                'feature_index': dim,
                'similarity': float(max(0, similarity)),
                'spread': float(spread),
            })

        feature_similarities.sort(key=lambda x: x['similarity'], reverse=True)

        top_features = feature_similarities[:3]

        return {
            'top_features': top_features,
            'all_feature_similarities': feature_similarities,
            'most_similar_dimension': feature_similarities[0]['feature_index'] if feature_similarities else None,
            'least_similar_dimension': feature_similarities[-1]['feature_index'] if feature_similarities else None,
        }

    def _compute_stability_score(self, centroids: np.ndarray, centroid_meta: list,
                                  base_threshold: float) -> Dict[str, Any]:
        multipliers = [0.8, 0.9, 1.0, 1.1, 1.2, 1.3, 1.5]
        cross_org_counts = []

        for mult in multipliers:
            threshold = base_threshold * mult
            if len(centroids) < 2:
                cross_org_counts.append(0)
                continue

            labels = AgglomerativeClustering(
                n_clusters=None, distance_threshold=threshold, linkage='average'
            ).fit_predict(centroids)

            clusters = defaultdict(list)
            for idx, lbl in enumerate(labels):
                clusters[lbl].append(centroid_meta[idx])

            count = 0
            for members in clusters.values():
                org_types = set(m['node_type'] for m in members)
                if len(org_types) > 1:
                    count += 1
            cross_org_counts.append(count)

        has_cross_org = [1 if c > 0 else 0 for c in cross_org_counts]
        stability = sum(has_cross_org) / len(multipliers)

        return {
            'stability_score': round(stability, 3),
            'multipliers_tested': multipliers,
            'cross_org_counts': cross_org_counts,
            'appears_at_n_thresholds': sum(has_cross_org),
            'total_thresholds_tested': len(multipliers),
            'interpretation': (
                "Very stable" if stability >= 0.85 else
                "Stable" if stability >= 0.7 else
                "Moderately stable" if stability >= 0.5 else
                "Unstable — may be coincidental"
            ),
        }

    def _compute_significance(self, centroids: np.ndarray, centroid_meta: list,
                               threshold: float, observed_cross_org: int,
                               n_permutations: int = 100) -> Dict[str, Any]:
        if observed_cross_org == 0 or len(centroids) < 4:
            return {
                'p_value': 1.0,
                'significant': False,
                'n_permutations': 0,
                'interpretation': 'No cross-org patterns to test',
            }

        random_counts = []
        org_labels = [m['node_type'] for m in centroid_meta]

        rng = np.random.RandomState(42)
        for _ in range(n_permutations):
            shuffled_labels = rng.permutation(org_labels)
            shuffled_meta = [{'node_type': lbl} for lbl in shuffled_labels]

            labels = AgglomerativeClustering(
                n_clusters=None, distance_threshold=threshold, linkage='average'
            ).fit_predict(centroids)

            clusters = defaultdict(list)
            for idx, lbl in enumerate(labels):
                clusters[lbl].append(shuffled_meta[idx])

            count = 0
            for members in clusters.values():
                org_types = set(m['node_type'] for m in members)
                if len(org_types) > 1:
                    count += 1
            random_counts.append(count)

        p_value = sum(1 for rc in random_counts if rc >= observed_cross_org) / n_permutations

        return {
            'p_value': round(p_value, 4),
            'significant': p_value < 0.05,
            'n_permutations': n_permutations,
            'observed_cross_org': observed_cross_org,
            'mean_random_cross_org': round(float(np.mean(random_counts)), 2),
            'max_random_cross_org': int(np.max(random_counts)) if random_counts else 0,
            'interpretation': (
                f"Highly significant (p={p_value:.4f})" if p_value < 0.01 else
                f"Significant (p={p_value:.4f})" if p_value < 0.05 else
                f"Not significant (p={p_value:.4f}) — patterns may be coincidental"
            ),
        }

    def extract_cross_org_insights(self) -> Dict[str, Any]:
        logger.info(f"\n{'='*60}\nCROSS-ORGANIZATIONAL INSIGHTS (EXTENDED)\n{'='*60}")

        insights = {
            'cross_org_clusters': [],
            'org_specific_patterns': [],
            'summary_stats': {},
            'similarity_metrics_used': ['euclidean', 'cosine', 'correlation'],
            'stability': {},
            'significance': {},
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
        n_features = all_centroids.shape[1]

        logger.info(f"Unified: {len(all_centroids)} centroids, "
                     f"{len(self.node_results)} nodes, "
                     f"{len(self._get_all_algorithms())} algorithms")

        scaler = StandardScaler()
        normalized_centroids = scaler.fit_transform(all_centroids)

        logger.info("Computing multi-metric distances (euclidean + cosine + correlation)...")
        combined_dist = self._compute_combined_distance(normalized_centroids)

        base_threshold = np.median(combined_dist[combined_dist > 0]) if np.any(combined_dist > 0) else 1.0
        threshold = base_threshold * 1.2
        logger.info(f"Combined threshold: {threshold:.3f} (base: {base_threshold:.3f})")

        labels = AgglomerativeClustering(
            n_clusters=None, distance_threshold=threshold,
            metric='precomputed', linkage='average'
        ).fit_predict(combined_dist)

        clusters = defaultdict(list)
        for idx, lbl in enumerate(labels):
            clusters[lbl].append({
                'centroid': all_centroids[idx],
                'normalized_centroid': normalized_centroids[idx],
                'weight': all_weights[idx],
                **centroid_meta[idx],
            })

        cross_org_count = 0
        org_specific_count = 0

        node_samples = {}
        for nr in self.node_results:
            node_samples[nr['node_id']] = nr['data_summary']['n_samples']

        total_data_points = sum(node_samples.values())

        multi_metrics = self._compute_multi_metric_distances(normalized_centroids)

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

                support = unique_points / total_data_points if total_data_points > 0 else 0

                member_indices = []
                for m in members:
                    for idx, cm in enumerate(centroid_meta):
                        if (cm['node_id'] == m['node_id'] and
                            cm['algorithm'] == m['algorithm'] and
                            cm['local_cluster_id'] == m['local_cluster_id']):
                            member_indices.append(idx)
                            break

                avg_metrics = {}
                for metric_name, dist_matrix in multi_metrics.items():
                    if len(member_indices) >= 2:
                        pair_dists = []
                        for i_idx in range(len(member_indices)):
                            for j_idx in range(i_idx + 1, len(member_indices)):
                                pair_dists.append(dist_matrix[member_indices[i_idx], member_indices[j_idx]])
                        avg_metrics[f'avg_{metric_name}_distance'] = round(float(np.mean(pair_dists)), 4)
                    else:
                        avg_metrics[f'avg_{metric_name}_distance'] = 0.0

                feature_analysis = self._analyze_feature_contributions(members, n_features)

                interpretation = self._build_extended_interpretation(
                    org_types=list(org_types),
                    algorithms=list(algorithms),
                    unique_points=unique_points,
                    cohesion=cohesion,
                    support=support,
                    feature_analysis=feature_analysis,
                    avg_metrics=avg_metrics,
                )

                insights['cross_org_clusters'].append({
                    'cluster_id': f'unified_cluster_{cid}',
                    'organizations': sorted(list(org_types)),
                    'nodes': sorted(list(node_ids)),
                    'algorithms': sorted(list(algorithms)),
                    'size': total_points,
                    'unique_data_points': unique_points,
                    'n_local_clusters': len(members),
                    'cohesion': cohesion,
                    'support': round(support, 4),
                    'similarity_metrics': avg_metrics,
                    'feature_analysis': feature_analysis,
                    'interpretation': interpretation,
                })

                logger.info(
                    f"✓ Cross-org: {', '.join(org_types)} | "
                    f"{len(algorithms)} algos | {unique_points} unique pts | "
                    f"cohesion={cohesion:.3f} | support={support:.1%}"
                )
            else:
                org_specific_count += 1
                insights['org_specific_patterns'].append({
                    'organization': list(org_types)[0],
                    'algorithms': sorted(list(algorithms)),
                    'size': total_points,
                    'cohesion': cohesion,
                })

        logger.info("Computing stability score across 7 thresholds...")
        insights['stability'] = self._compute_stability_score(
            normalized_centroids, centroid_meta, base_threshold
        )
        logger.info(f"  Stability: {insights['stability']['stability_score']} "
                     f"({insights['stability']['interpretation']})")

        logger.info("Computing statistical significance (100 permutations)...")
        insights['significance'] = self._compute_significance(
            normalized_centroids, centroid_meta, threshold, cross_org_count
        )
        logger.info(f"  p-value: {insights['significance']['p_value']} "
                     f"({insights['significance']['interpretation']})")

        insights['summary_stats'] = {
            'total_unified_clusters': len(clusters),
            'cross_org_clusters': cross_org_count,
            'org_specific_clusters': org_specific_count,
            'total_centroids_analyzed': len(all_centroids),
            'threshold_used': float(threshold),
            'metrics_used': 'euclidean (50%) + cosine (25%) + correlation (25%)',
            'centroids_normalized': True,
        }

        logger.info(f"\nResults: {len(clusters)} clusters, "
                     f"{cross_org_count} cross-org, {org_specific_count} org-specific")

        return insights

    def _build_extended_interpretation(self, org_types, algorithms, unique_points,
                                        cohesion, support, feature_analysis, avg_metrics):
        interpretation = (
            f"Unified pattern spanning {', '.join(sorted(org_types))} organizations, "
            f"detected across {len(algorithms)} algorithm(s) ({', '.join(sorted(algorithms))}). "
            f"Involves {unique_points:,} unique data points. "
        )

        if cohesion > 0.7:
            interpretation += "Very high cohesion — strong structural similarity between organizations. "
        elif cohesion > 0.4:
            interpretation += "Moderate cohesion — shared characteristics detected. "
        else:
            interpretation += "Low cohesion — loose similarity. "

        if support > 0.8:
            interpretation += f"High support ({support:.0%} of all data) — pattern covers most of the dataset. "
        elif support > 0.5:
            interpretation += f"Moderate support ({support:.0%} of all data). "
        else:
            interpretation += f"Low support ({support:.0%} of all data) — affects a subset. "

        cos_dist = avg_metrics.get('avg_cosine_distance', 1)
        if cos_dist < 0.3:
            interpretation += "Strong directional similarity (cosine) — organizations show similar feature proportions. "
        elif cos_dist < 0.6:
            interpretation += "Moderate directional similarity between organizations. "

        corr_dist = avg_metrics.get('avg_correlation_distance', 1)
        if corr_dist < 0.3:
            interpretation += "High correlation — feature patterns move together across organizations. "

        top_feats = feature_analysis.get('top_features', [])
        if top_feats:
            top_dims = [f"dimension {f['feature_index']}" for f in top_feats[:2]]
            interpretation += (
                f"Most similar features: {', '.join(top_dims)} "
                f"(similarity: {top_feats[0]['similarity']:.0%}). "
            )

        orgs = set(org_types)
        if 'healthcare' in orgs and 'retail' in orgs:
            interpretation += "Health demographics correlate with consumer purchasing patterns. "
        if 'iot' in orgs and 'healthcare' in orgs:
            interpretation += "Environmental sensor data correlates with health metrics. "
        if 'retail' in orgs and 'iot' in orgs:
            interpretation += "Consumer behavior aligns with environmental conditions. "
        if len(orgs) == 3:
            interpretation += "GLOBAL pattern spanning all three domains — significant finding. "

        return interpretation

    def generate_pca_visualization(self) -> Dict[str, Any]:
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
        logger.info(f"  Stability: {report['insights'].get('stability', {}).get('stability_score', 'N/A')}")
        logger.info(f"  Significance: p={report['insights'].get('significance', {}).get('p_value', 'N/A')}")

        return report