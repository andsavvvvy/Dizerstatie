"""
PDF Report Generator for Distributed Clustering Analysis
Generates a professional PDF with tables and charts.
Requires: pip install reportlab
"""
import io
from datetime import datetime
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import mm, cm
from reportlab.lib.enums import TA_LEFT
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    PageBreak, HRFlowable, Image
)
from reportlab.graphics.shapes import Drawing, Rect, String, Line
from reportlab.graphics.charts.barcharts import VerticalBarChart
from reportlab.graphics import renderPDF
import logging

logger = logging.getLogger(__name__)

WIDTH, HEIGHT = A4


def _make_table_style(header_color='#0d6efd'):
    return TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor(header_color)),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 8),
        ('FONTSIZE', (0, 1), (-1, -1), 7.5),
        ('GRID', (0, 0), (-1, -1), 0.4, colors.HexColor('#dee2e6')),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f8f9fa')]),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('LEFTPADDING', (0, 0), (-1, -1), 4),
        ('RIGHTPADDING', (0, 0), (-1, -1), 4),
        ('TOPPADDING', (0, 0), (-1, -1), 3),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
    ])


def _para(text, style):
    """Helper to wrap text in Paragraph for table cells (auto line-break)."""
    return Paragraph(text, style)


def _build_bar_chart(names, values, title_text, width=450, height=200,
                     bar_color=colors.HexColor('#0d6efd'),
                     best_color=colors.HexColor('#198754'), best_name=None):
    """Create a simple vertical bar chart as a Drawing."""
    d = Drawing(width, height + 30)
    d.add(String(width / 2, height + 15, title_text,
                 fontSize=10, fillColor=colors.HexColor('#333'),
                 textAnchor='middle', fontName='Helvetica-Bold'))

    chart = VerticalBarChart()
    chart.x = 60
    chart.y = 30
    chart.width = width - 90
    chart.height = height - 40
    chart.data = [values]
    chart.categoryAxis.categoryNames = names
    chart.categoryAxis.labels.fontSize = 6
    chart.categoryAxis.labels.angle = 30
    chart.categoryAxis.labels.boxAnchor = 'ne'
    chart.valueAxis.labels.fontSize = 7
    chart.valueAxis.valueMin = min(0, min(values) - 0.05) if values else 0
    chart.valueAxis.valueMax = max(values) * 1.15 if values else 1
    chart.bars[0].fillColor = bar_color
    if best_name and best_name in names:
        for i, n in enumerate(names):
            if n == best_name:
                chart.bars[0].fillColor = bar_color  # default
    
    d.add(chart)
    return d


def _build_grouped_bar_chart(categories, series_data, series_names, title_text,
                              width=450, height=220):
    """Grouped bar chart with multiple series."""
    d = Drawing(width, height + 30)
    d.add(String(width / 2, height + 15, title_text,
                 fontSize=10, fillColor=colors.HexColor('#333'),
                 textAnchor='middle', fontName='Helvetica-Bold'))

    chart = VerticalBarChart()
    chart.x = 60
    chart.y = 30
    chart.width = width - 90
    chart.height = height - 50
    chart.data = series_data
    chart.categoryAxis.categoryNames = categories
    chart.categoryAxis.labels.fontSize = 6
    chart.categoryAxis.labels.angle = 30
    chart.categoryAxis.labels.boxAnchor = 'ne'
    chart.valueAxis.labels.fontSize = 7
    chart.bars.strokeWidth = 0
    chart.groupSpacing = 8

    palette = [
        colors.HexColor('#0d6efd'), colors.HexColor('#198754'),
        colors.HexColor('#ffc107'), colors.HexColor('#dc3545'),
        colors.HexColor('#6f42c1'),
    ]
    for i, name in enumerate(series_names):
        chart.bars[i].fillColor = palette[i % len(palette)]
        chart.bars[i].name = name
    for i, name in enumerate(series_names):
        x_pos = 70 + i * 120
        d.add(Rect(x_pos, 5, 10, 8, fillColor=palette[i % len(palette)], strokeColor=None))
        d.add(String(x_pos + 14, 5, name, fontSize=7, fillColor=colors.black))

    d.add(chart)
    return d


def generate_analysis_pdf(analysis, node_participation=None, local_by_node=None, node_performance=None):
    """Generate PDF report. Returns bytes."""
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(
        buffer, pagesize=A4,
        rightMargin=1.5 * cm, leftMargin=1.5 * cm,
        topMargin=1.5 * cm, bottomMargin=1.5 * cm,
    )

    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle('MainTitle', parent=styles['Heading1'],
                               fontSize=20, spaceAfter=2*mm, textColor=colors.HexColor('#0d6efd'),
                               alignment=TA_LEFT))
    styles.add(ParagraphStyle('SubTitle', parent=styles['Heading2'],
                               fontSize=14, spaceAfter=4*mm, alignment=TA_LEFT))
    styles.add(ParagraphStyle('Section', parent=styles['Heading2'],
                               fontSize=13, spaceBefore=6*mm, spaceAfter=3*mm,
                               textColor=colors.HexColor('#198754'), alignment=TA_LEFT))
    styles.add(ParagraphStyle('SubSec', parent=styles['Heading3'],
                               fontSize=10, spaceBefore=3*mm, spaceAfter=2*mm))
    styles.add(ParagraphStyle('Small', parent=styles['Normal'], fontSize=8, textColor=colors.grey))
    styles.add(ParagraphStyle('CellText', parent=styles['Normal'], fontSize=7.5, leading=9))

    elements = []
    elements.append(Spacer(1, 1 * cm))
    elements.append(Paragraph("Distributed Clustering — Analysis Report", styles['MainTitle']))
    elements.append(HRFlowable(width="100%", thickness=2, color=colors.HexColor('#0d6efd')))
    elements.append(Spacer(1, 6 * mm))
    elements.append(Paragraph(f"Session: {analysis['session_id']}", styles['Small']))
    elements.append(Paragraph(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", styles['Small']))
    elements.append(Spacer(1, 8 * mm))

    created = analysis.get('created_at', 'N/A')
    completed = analysis.get('completed_at', 'N/A')
    if hasattr(created, 'strftime'): created = created.strftime('%Y-%m-%d %H:%M:%S')
    if hasattr(completed, 'strftime'): completed = completed.strftime('%Y-%m-%d %H:%M:%S')
    exec_ms = analysis.get('execution_time_total_ms', 0)
    exec_str = f"{exec_ms/1000:.1f}s" if exec_ms else 'N/A'

    summary = [
        ['Metric', 'Value'],
        ['Status', analysis.get('status', '').upper()],
        ['Total Nodes', str(analysis.get('total_nodes', 0))],
        ['Total Data Points', f"{analysis.get('total_data_points', 0):,}"],
        ['Best Algorithm', analysis.get('best_algorithm', 'N/A')],
        ['Best Silhouette Score', f"{analysis.get('best_algorithm_score', 0):.4f}"],
        ['Started', str(created)],
        ['Completed', str(completed)],
        ['Duration', exec_str],
    ]
    t = Table(summary, colWidths=[5*cm, 11*cm])
    t.setStyle(_make_table_style('#0d6efd'))
    elements.append(t)
    elements.append(PageBreak())
    elements.append(Paragraph("Algorithm Performance", styles['Section']))

    ensemble = analysis.get('ensemble_analysis', {})
    algo_scores = ensemble.get('algorithm_scores', {})
    best_algo_name = analysis.get('best_algorithm', '')

    if algo_scores:
        sorted_algos = sorted(algo_scores.items(), key=lambda x: x[1].get('avg_silhouette', 0), reverse=True)
        chart_names = [a[0] for a in sorted_algos]
        chart_vals = [a[1].get('avg_silhouette', 0) for a in sorted_algos]
        elements.append(_build_bar_chart(
            chart_names, chart_vals,
            'Average Silhouette Score by Algorithm',
            best_name=best_algo_name,
        ))
        elements.append(Spacer(1, 4 * mm))
        algo_data = [['Algorithm', 'Avg Silhouette', 'Std Dev', 'Consistency', 'Avg Clusters', 'Nodes']]
        for algo, scores in sorted_algos:
            is_best = algo == best_algo_name
            name_text = f"<b>★ {algo}</b>" if is_best else algo
            algo_data.append([
                _para(name_text, styles['CellText']),
                f"{scores.get('avg_silhouette', 0):.4f}",
                f"{scores.get('std_silhouette', 0):.4f}",
                f"{scores.get('consistency', 0):.4f}",
                f"{scores.get('avg_clusters', 0):.1f}",
                str(scores.get('nodes_count', 0)),
            ])

        t = Table(algo_data, colWidths=[4*cm, 2.5*cm, 2*cm, 2.3*cm, 2.3*cm, 1.5*cm])
        t.setStyle(_make_table_style('#198754'))
        elements.append(t)
        elements.append(Spacer(1, 4 * mm))

        rec = ensemble.get('recommendation', '')
        if rec:
            elements.append(Paragraph("<b>Recommendation:</b> " + rec, styles['Normal']))

    elements.append(PageBreak())
    if node_participation:
        elements.append(Paragraph("Node Participation", styles['Section']))

        np_data = [['Node', 'Type', 'Data Points', 'Contribution', 'Best Algorithm', 'Score']]
        for np_item in node_participation:
            np_data.append([
                _para(np_item.get('node_id', ''), styles['CellText']),
                np_item.get('node_type', ''),
                f"{np_item.get('data_points_contributed', 0):,}",
                f"{np_item.get('contribution_weight', 0) * 100:.1f}%",
                np_item.get('best_local_algorithm', ''),
                f"{np_item.get('best_local_score', 0):.4f}",
            ])
        t = Table(np_data, colWidths=[3.8*cm, 2*cm, 2.3*cm, 2.3*cm, 3.2*cm, 1.8*cm])
        t.setStyle(_make_table_style('#0dcaf0'))
        elements.append(t)
        elements.append(Spacer(1, 6 * mm))
    if local_by_node:
        elements.append(Paragraph("Per-Node Algorithm Breakdown", styles['Section']))

        for node_id, algos in local_by_node.items():
            elements.append(Paragraph(f"Node: {node_id}", styles['SubSec']))

            lr_data = [['Algorithm', 'Clusters', 'Silhouette', 'Davies-Bouldin', 'Time (ms)']]
            for lr in algos:
                sil = lr.get('silhouette_score', 0) or 0
                db = lr.get('davies_bouldin_score')
                lr_data.append([
                    _para(lr.get('algorithm', ''), styles['CellText']),
                    str(lr.get('n_local_clusters', 0)),
                    f"{sil:.4f}",
                    f"{db:.4f}" if db else 'N/A',
                    str(lr.get('execution_time_ms', 0)),
                ])
            t = Table(lr_data, colWidths=[4*cm, 2*cm, 2.5*cm, 2.8*cm, 2.5*cm])
            t.setStyle(_make_table_style('#6c757d'))
            elements.append(t)
            elements.append(Spacer(1, 3 * mm))
        all_algos_set = set()
        for algos in local_by_node.values():
            for lr in algos:
                all_algos_set.add(lr['algorithm'])
        all_algos_sorted = sorted(all_algos_set)
        node_ids = list(local_by_node.keys())

        if all_algos_sorted and node_ids:
            series_data = []
            for nid in node_ids:
                row = []
                algo_map = {lr['algorithm']: lr for lr in local_by_node[nid]}
                for algo in all_algos_sorted:
                    if algo in algo_map:
                        row.append(algo_map[algo].get('silhouette_score', 0) or 0)
                    else:
                        row.append(0)
                series_data.append(row)

            elements.append(Spacer(1, 4 * mm))
            elements.append(_build_grouped_bar_chart(
                all_algos_sorted, series_data, node_ids,
                'Silhouette Score per Algorithm per Node',
            ))
        if all_algos_sorted and node_ids:
            time_series = []
            for nid in node_ids:
                row = []
                algo_map = {lr['algorithm']: lr for lr in local_by_node[nid]}
                for algo in all_algos_sorted:
                    if algo in algo_map:
                        row.append(algo_map[algo].get('execution_time_ms', 0))
                    else:
                        row.append(0)
                time_series.append(row)

            elements.append(Spacer(1, 6 * mm))
            elements.append(_build_grouped_bar_chart(
                all_algos_sorted, time_series, node_ids,
                'Execution Time (ms) per Algorithm per Node',
            ))

    elements.append(PageBreak())
    if node_performance:
        elements.append(Paragraph("System Performance", styles['Section']))

        perf_data = [['Node', 'CPU %', 'Memory (MB)', 'Avg Silhouette', 'Algorithms', 'Avg Time (ms)']]
        for nid, perf in node_performance.items():
            cpu = perf.get('cpu_usage_percent')
            mem = perf.get('memory_usage_mb')
            perf_data.append([
                _para(nid, styles['CellText']),
                f"{cpu:.1f}%" if cpu is not None else 'N/A',
                f"{mem:.1f}" if mem is not None else 'N/A',
                f"{perf.get('avg_silhouette_7d', 0):.4f}",
                str(perf.get('total_analyses_7d', 0)),
                str(perf.get('avg_execution_time_ms', 0)),
            ])
        t = Table(perf_data, colWidths=[3.8*cm, 1.8*cm, 2.3*cm, 2.5*cm, 2*cm, 2.8*cm])
        t.setStyle(_make_table_style('#ffc107'))
        t.setStyle(TableStyle([('TEXTCOLOR', (0, 0), (-1, 0), colors.black)]))
        elements.append(t)
        elements.append(Spacer(1, 6 * mm))
    ci = analysis.get('cross_org_insights', {})
    cross_org = ci.get('cross_org_clusters', [])

    elements.append(Paragraph("Cross-Organizational Insights", styles['Section']))

    stats = ci.get('summary_stats', {})
    if stats:
        stats_data = [
            ['Centroids Analyzed', 'Unified Clusters', 'Cross-Org Patterns', 'Org-Specific'],
            [
                str(stats.get('total_centroids_analyzed', 0)),
                str(stats.get('total_unified_clusters', 0)),
                str(stats.get('cross_org_clusters', 0)),
                str(stats.get('org_specific_clusters', 0)),
            ],
        ]
        t = Table(stats_data, colWidths=[3.8*cm, 3.5*cm, 3.8*cm, 3*cm])
        t.setStyle(_make_table_style('#6f42c1'))
        t.setStyle(TableStyle([('ALIGN', (0, 0), (-1, -1), 'CENTER')]))
        elements.append(t)
        elements.append(Spacer(1, 4 * mm))

    if cross_org:
        for i, insight in enumerate(cross_org, 1):
            elements.append(Paragraph(f"<b>Pattern {i}</b>", styles['SubSec']))
            elements.append(Paragraph(
                f"<b>Organizations:</b> {', '.join(insight.get('organizations', []))}", styles['Normal']))
            elements.append(Paragraph(
                f"<b>Nodes:</b> {', '.join(insight.get('nodes', []))}", styles['Normal']))
            elements.append(Paragraph(
                f"<b>Algorithms:</b> {', '.join(insight.get('algorithms', []))}", styles['Normal']))
            elements.append(Paragraph(
                f"<b>Unique Data Points:</b> {insight.get('unique_data_points', 0):,} &nbsp;|&nbsp; "
                f"<b>Cohesion:</b> {insight.get('cohesion', 0):.3f}", styles['Normal']))
            elements.append(Spacer(1, 2 * mm))
            elements.append(Paragraph(insight.get('interpretation', ''), styles['Normal']))
            elements.append(Spacer(1, 5 * mm))
    else:
        elements.append(Paragraph("No cross-organizational patterns detected.", styles['Normal']))
    elements.append(Spacer(1, 4 * mm))
    elements.append(HRFlowable(width="100%", thickness=1, color=colors.HexColor('#dee2e6')))
    elements.append(Spacer(1, 4 * mm))
    elements.append(Paragraph("Key Findings", styles['Section']))

    n_algos = len(algo_scores)
    findings = [
        f"Best performing algorithm: <b>{analysis.get('best_algorithm', 'N/A')}</b> "
        f"(silhouette: {analysis.get('best_algorithm_score', 0):.3f})",
        f"Total data points analyzed: <b>{analysis.get('total_data_points', 0):,}</b> "
        f"across {analysis.get('total_nodes', 0)} distributed nodes",
        f"Algorithms evaluated: <b>{n_algos}</b>",
        f"Cross-organizational patterns: <b>{len(cross_org)}</b>",
        f"Analysis duration: <b>{exec_str}</b>",
    ]
    for f in findings:
        elements.append(Paragraph(f"• {f}", styles['Normal']))
        elements.append(Spacer(1, 1.5 * mm))
    doc.build(elements)
    buffer.seek(0)
    return buffer.getvalue()