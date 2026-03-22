# Sistem Distribuit de Data Mining prin Clustering

Sistem distribuit de clustering care procesează date pe noduri independente (medical, retail, IoT) și agregă rezultatele prin meta-clustering și analiză ensemble.

Proiect de disertație — Administrarea Bazelor de Date, 2025–2026.

---

## Arhitectură

```
┌──────────────────────────────────────────────────┐
│              Web Dashboard (Port 9000)           │
└────────────────────┬─────────────────────────────┘
                     │
┌────────────────────▼─────────────────────────────┐
│          Orchestrator Global (Port 7000)         │
│     Meta-clustering · Ensemble · PCA · Insights  │
└──────┬─────────────┬─────────────┬───────────────┘
       │             │             │
┌──────▼───┐  ┌──────▼───┐  ┌─────▼────┐  ┌──────────┐
│ Medical  │  │  Retail  │  │   IoT    │  │ MySQL DB │
│  :6001   │  │  :6002   │  │  :6003   │  │  :3306   │
│ KMEANS   │  │ GMM      │  │ SPECTRAL │  │ 7 tabele │
│ DBSCAN   │  │ BIRCH    │  │ AFFINITY │  │          │
│ AGGLO    │  │ MEANSHIFT│  │ MINIBATCH│  │          │
└──────────┘  └──────────┘  └──────────┘  └──────────┘
```

Fiecare nod procesează datele local și trimite doar centroizii la orchestrator. Datele brute nu părăsesc nodul.

---

## Funcționalități

- 9 algoritmi de clustering distribuiți pe 3 noduri (3 per nod)
- Meta-clustering pe centroizi cu AgglomerativeClustering
- Analiză ensemble cu selecție automată a celui mai bun algoritm
- Detecție cross-organizațională prin unified meta-clustering
- Vizualizare PCA 2D a centroizilor (Plotly interactiv)
- Evaluare comparativă: quality vs speed, heatmap, clustere, timpi
- Export PDF al raportului complet
- Dataset Manager: upload CSV/XLSX/JSON, asignare per nod, seturi default protejate
- Monitorizare sistem: CPU, RAM, silhouette, Davies-Bouldin per nod
- Dashboard web cu 7 tab-uri de analiză

---

## Instalare

```bash
python -m venv .venv
.venv\Scripts\activate        # Windows
source .venv/bin/activate     # Linux/Mac

pip install -r requirements.txt
```

Configurează baza de date în `.env`:

```
DB_HOST=localhost
DB_USER=root
DB_PASSWORD=parola
DB_NAME=distributed_clustering
```

Rulează schema:

```bash
mysql -u root -p < db/schema_distributed.sql
```

---

## Pornire

5 terminale separate:

```bash
python orchestrator_global/app.py          # Orchestrator  :7000
python nodes/node_medical/local_miner.py   # Medical       :6001
python nodes/node_retail/local_miner.py    # Retail        :6002
python nodes/node_iot/local_miner.py       # IoT           :6003
python ui/app.py                           # Dashboard     :9000
```

Deschide http://localhost:9000

---

## Structura Proiectului

```
├── nodes/
│   ├── base_node.py              # Clasă abstractă (9 algoritmi, psutil, DB loading)
│   ├── node_medical/             # Nod healthcare (KMEANS, DBSCAN, AGGLO)
│   ├── node_retail/              # Nod retail (GMM, BIRCH, MEANSHIFT)
│   └── node_iot/                 # Nod IoT (SPECTRAL, AFFINITY_PROP, MINIBATCH)
├── orchestrator_global/
│   ├── app.py                    # API Flask orchestrator
│   └── aggregation_engine.py     # Meta-clustering, ensemble, PCA, cross-org
├── ui/
│   ├── app.py                    # Web interface Flask
│   ├── pdf_generator.py          # Export PDF cu reportlab
│   ├── templates/                # Jinja2 templates
│   └── static/                   # CSS, JS (Plotly charts)
├── db/
│   ├── connection.py             # Conexiune MySQL
│   ├── repository.py             # CRUD complet (7 tabele)
│   └── schema_distributed.sql    # Schema completă
└── requirements.txt
```

---

## Baza de Date (7 tabele)

| Tabelă | Rol |
|---|---|
| `distributed_nodes` | Noduri înregistrate |
| `global_analyses` | Sesiuni de analiză + rezultate JSON + PCA |
| `node_local_results` | Rezultate per nod, per algoritm |
| `analysis_node_participation` | Contribuția fiecărui nod |
| `node_performance_metrics` | CPU, RAM, silhouette, timp |
| `datasets` | Fișiere uploadate (BLOB), cu flag `is_default` |
| `node_dataset_assignments` | Ce dataset e activ pe ce nod |

---

## Tehnologii

- Python 3.8+, Flask, Jinja2
- scikit-learn (9 algoritmi), scipy, numpy, pandas
- MySQL 8.0, mysql-connector-python
- Plotly.js (vizualizări interactive)
- Bootstrap 5 (UI responsiv)
- reportlab (generare PDF)
- psutil (monitorizare sistem)

---

## Autor

Săvulescu Andrei
Disertație — Administrarea Bazelor de Date
Profesor Coordonator — Florin Radulescu
Facultatea de Automatica si Calculatoare
Universitatea Națională de Știință și Tehnologie Politehnica București, 2025–2026