"""
IoT Node - Sensor Data Clustering
Specializes in time-series sensor data and anomaly detection
"""
from flask import Flask, jsonify, request
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from nodes.base_node import BaseNode
import numpy as np
import pandas as pd
from sklearn.preprocessing import RobustScaler

class IoTNode(BaseNode):
    """
    IoT specialized node
    
    Data characteristics:
    - Sensor readings (temperature, humidity, etc.)
    - High outlier rate (sensor failures)
    - RobustScaler preferred (resistant to outliers)
    """
    
    def load_local_data(self) -> np.ndarray:
        """Load IoT sensor data"""
        
        data_path = self.config['data_path']
        
        self.logger.info(f"Loading IoT data from {data_path}")
        
        try:
            df = pd.read_csv(data_path)
            
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            data = df[numeric_cols].values
            
            self.logger.info(
                f"Loaded {len(data)} sensor readings with {len(numeric_cols)} metrics"
            )
            
            return data
            
        except FileNotFoundError:
            self.logger.error(f"Data file not found: {data_path}")
            self.logger.info("Generating sample IoT data...")
            return self._generate_sample_data()
    
    def _generate_sample_data(self, n_samples: int = 800) -> np.ndarray:
        """
        Generate sample IoT sensor data
        Features: Temperature, Humidity, CO2, Light, Noise, Motion
        """
        np.random.seed(44)
        
        # 3 environmental conditions + anomalies
        
        # Condition 1: Normal office (50%)
        n1 = int(n_samples * 0.5)
        normal = np.random.multivariate_normal(
            mean=[22, 45, 400, 300, 40, 0.3],  # Temp(C), Humidity(%), CO2(ppm), Light(lux), Noise(dB), Motion
            cov=np.diag([4, 25, 2500, 10000, 25, 0.04]),
            size=n1
        )
        
        # Condition 2: High occupancy (30%)
        n2 = int(n_samples * 0.3)
        occupied = np.random.multivariate_normal(
            mean=[24, 55, 800, 500, 60, 0.8],
            cov=np.diag([9, 36, 10000, 22500, 64, 0.09]),
            size=n2
        )
        
        # Condition 3: Night/empty (15%)
        n3 = int(n_samples * 0.15)
        empty = np.random.multivariate_normal(
            mean=[19, 40, 300, 50, 25, 0.05],
            cov=np.diag([4, 16, 1600, 625, 9, 0.01]),
            size=n3
        )
        
        # Anomalies: Sensor failures (5%)
        n4 = n_samples - n1 - n2 - n3
        anomalies = np.random.multivariate_normal(
            mean=[30, 80, 2000, 1000, 90, 1.0],
            cov=np.diag([100, 400, 1000000, 250000, 400, 0.25]),
            size=n4
        )
        
        # Combine
        data = np.vstack([normal, occupied, empty, anomalies])
        
        # Add sensor failures (missing readings) (7%)
        mask = np.random.random(data.shape) < 0.07
        data[mask] = np.nan
        
        # Realistic ranges
        data[:, 0] = np.clip(data[:, 0], -10, 50)  # Temperature
        data[:, 1] = np.clip(data[:, 1], 0, 100)  # Humidity
        data[:, 2] = np.clip(data[:, 2], 200, 5000)  # CO2
        data[:, 3] = np.clip(data[:, 3], 0, 2000)  # Light
        data[:, 4] = np.clip(data[:, 4], 0, 120)  # Noise
        data[:, 5] = np.clip(data[:, 5], 0, 1)  # Motion (binary-ish)
        
        self.logger.info(f"Generated {n_samples} sample sensor readings")
        
        # Save
        df = pd.DataFrame(
            data,
            columns=['Temperature', 'Humidity', 'CO2', 'Light', 'Noise', 'Motion']
        )
        
        os.makedirs(os.path.dirname(self.config['data_path']), exist_ok=True)
        df.to_csv(self.config['data_path'], index=False)
        self.logger.info(f"Saved sample data to {self.config['data_path']}")
        
        return data
    
    def preprocess_data(self, data: np.ndarray) -> np.ndarray:
        """
        IoT-specific preprocessing
        
        - Forward fill for missing (time-series nature)
        - RobustScaler (resistant to outliers/sensor failures)
        """
        self.logger.info("Preprocessing IoT sensor data...")
        
        # Convert to DataFrame for easier manipulation
        df = pd.DataFrame(data)
        
        # Forward fill (assume sensors report last known value)
        df = df.ffill()
        
        # If still NaN (first rows), use backward fill
        df = df.bfill()
        
        # Convert back to numpy
        data = df.values
        
        # RobustScaler: uses median and IQR (resistant to outliers)
        scaler = RobustScaler()
        data = scaler.fit_transform(data)
        
        self.logger.info("Applied RobustScaler (outlier-resistant)")
        
        return data


# ============================================
# Flask API
# ============================================

app = Flask(__name__)

config_path = os.path.join(os.path.dirname(__file__), 'config.json')
iot_node = IoTNode(config_path)

@app.route('/health', methods=['GET'])
def health():
    return jsonify({
        'status': 'healthy',
        'node_id': iot_node.node_id,
        'node_type': iot_node.node_type
    })

@app.route('/status', methods=['GET'])
def status():
    return jsonify(iot_node.get_status())

@app.route('/local_cluster', methods=['POST'])
def local_cluster():
    try:
        results = iot_node.run_local_clustering()
        return jsonify({
            'status': 'success',
            'results': results
        })
    except Exception as e:
        iot_node.logger.error(f"Clustering failed: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@app.route('/send_to_global', methods=['POST'])
def send_to_global():
    try:
        session_id = request.json.get('session_id')
        
        if not session_id:
            return jsonify({
                'status': 'error',
                'message': 'session_id required'
            }), 400
        
        iot_node.run_local_clustering()
        response = iot_node.send_to_global_orchestrator(session_id)
        
        return jsonify({
            'status': 'success',
            'global_response': response
        })
        
    except Exception as e:
        iot_node.logger.error(f"Send to global failed: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

if __name__ == '__main__':
    print(f"""
╔═══════════════════════════════════════════════════════════════╗
║                                                               ║
║                      IoT NODE                                 ║
║                                                               ║
║  Node ID: {iot_node.node_id:50s} ║
║  Type:    {iot_node.node_type:50s} ║
║  Port:    {iot_node.port:<50d} ║
║                                                               ║
╚═══════════════════════════════════════════════════════════════╝
    """)
    
    app.run(
        host='0.0.0.0',
        port=iot_node.port,
        debug=False
    )