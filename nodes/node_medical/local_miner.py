"""
Medical Node - Patient Health Data Clustering
Specializes in healthcare metrics and patient segmentation
"""
from flask import Flask, jsonify, request
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from nodes.base_node import BaseNode
import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer


class MedicalNode(BaseNode):
    """
    Nod specializat pentru date medicale
    """
    
    def load_local_data(self) -> np.ndarray:
        """
        Încarcă date medicale din CSV local
        """
        data_path = self.config['data_path']
        
        self.logger.info(f"Loading medical data from {data_path}")
        
        try:
            df = pd.read_csv(data_path)
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            data = df[numeric_cols].values
            
            self.logger.info(
                f"Loaded {len(data)} patient records with {len(numeric_cols)} features"
            )
            
            return data
            
        except FileNotFoundError:
            self.logger.error(f"Data file not found: {data_path}")
            self.logger.info("Generating sample medical data...")
            return self._generate_sample_data()
    
    def _generate_sample_data(self, n_samples: int = 500) -> np.ndarray:
        """
        Generate sample medical data
        Features: Age, BMI, BloodPressure, Cholesterol, HeartRate
        """
        np.random.seed(42)
        n1 = int(n_samples * 0.4)
        young_healthy = np.random.multivariate_normal(
            mean=[30, 22, 115, 180, 70],
            cov=np.diag([25, 4, 100, 400, 16]),
            size=n1
        )
        n2 = int(n_samples * 0.35)
        middle_risk = np.random.multivariate_normal(
            mean=[50, 28, 135, 230, 78],
            cov=np.diag([36, 9, 225, 900, 25]),
            size=n2
        )
        n3 = n_samples - n1 - n2
        elderly = np.random.multivariate_normal(
            mean=[70, 26, 150, 260, 72],
            cov=np.diag([25, 6, 400, 1600, 36]),
            size=n3
        )
        data = np.vstack([young_healthy, middle_risk, elderly])
        mask = np.random.random(data.shape) < 0.02
        data[mask] = np.nan
        data[:, 0] = np.clip(data[:, 0], 18, 95)    # Age
        data[:, 1] = np.clip(data[:, 1], 15, 45)     # BMI
        data[:, 2] = np.clip(data[:, 2], 80, 200)    # Blood Pressure
        data[:, 3] = np.clip(data[:, 3], 100, 400)   # Cholesterol
        data[:, 4] = np.clip(data[:, 4], 45, 120)    # Heart Rate
        
        self.logger.info(f"Generated {n_samples} sample patient records")
        df = pd.DataFrame(
            data,
            columns=['Age', 'BMI', 'BloodPressure', 'Cholesterol', 'HeartRate']
        )
        
        os.makedirs(os.path.dirname(self.config['data_path']), exist_ok=True)
        df.to_csv(self.config['data_path'], index=False)
        self.logger.info(f"Saved sample data to {self.config['data_path']}")
        
        return data
    
    def preprocess_data(self, data: np.ndarray) -> np.ndarray:
        """
        Preprocessing specific medical:
        - Normalizare Z-score (importante valori absolute)
        - Tratare valori lipsă
        """
        self.logger.info("Preprocessing medical data...")
        imputer = SimpleImputer(strategy='median')
        data = imputer.fit_transform(data)
        scaler = StandardScaler()
        data = scaler.fit_transform(data)
        
        self.logger.info("Applied StandardScaler (Z-score normalization)")
        
        return data

app = Flask(__name__)

config_path = os.path.join(os.path.dirname(__file__), 'config.json')
medical_node = MedicalNode(config_path)

@app.route('/health', methods=['GET'])
def health():
    return jsonify({
        'status': 'healthy',
        'node_id': medical_node.node_id,
        'node_type': medical_node.node_type
    })

@app.route('/status', methods=['GET'])
def status():
    return jsonify(medical_node.get_status())

@app.route('/local_cluster', methods=['POST'])
def local_cluster():
    """
    Declanșează clustering local
    """
    try:
        results = medical_node.run_local_clustering()
        return jsonify({
            'status': 'success',
            'results': results
        })
    except Exception as e:
        medical_node.logger.error(f"Clustering failed: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@app.route('/send_to_global', methods=['POST'])
def send_to_global():
    """
    Rulează clustering și trimite la orchestrator global
    """
    try:
        session_id = request.json.get('session_id')
        
        if not session_id:
            return jsonify({
                'status': 'error',
                'message': 'session_id required'
            }), 400
        medical_node.run_local_clustering()
        response = medical_node.send_to_global_orchestrator(session_id)
        
        return jsonify({
            'status': 'success',
            'global_response': response
        })
        
    except Exception as e:
        medical_node.logger.error(f"Send to global failed: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

if __name__ == '__main__':
    print(f"""
╔═══════════════════════════════════════════════════════════════╗
║                                                               ║
║                   MEDICAL NODE                                ║
║                                                               ║
║  Node ID: {medical_node.node_id:50s} ║
║  Type:    {medical_node.node_type:50s} ║
║  Port:    {medical_node.port:<50d} ║
║                                                               ║
╚═══════════════════════════════════════════════════════════════╝
    """)
    
    app.run(
        host='0.0.0.0',
        port=medical_node.port,
        debug=False
    )
