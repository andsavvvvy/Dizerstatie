"""
Retail Node - Customer & Sales Data Clustering
Specializes in customer behavior and transaction patterns
"""
from flask import Flask, jsonify, request
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from nodes.base_node import BaseNode
import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from sklearn.impute import SimpleImputer

class RetailNode(BaseNode):
    """
    Retail specialized node
    
    Data characteristics:
    - Customer transactions, purchase amounts, frequency
    - Bounded ranges (amounts >= 0)
    - MinMax scaling preferred (preserves 0 values)
    """
    
    def load_local_data(self) -> np.ndarray:
        """Load retail data from local CSV"""
        
        data_path = self.config['data_path']
        
        self.logger.info(f"Loading retail data from {data_path}")
        
        try:
            df = pd.read_csv(data_path)
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            data = df[numeric_cols].values
            
            self.logger.info(
                f"Loaded {len(data)} customer records with {len(numeric_cols)} features"
            )
            
            return data
            
        except FileNotFoundError:
            self.logger.error(f"Data file not found: {data_path}")
            self.logger.info("Generating sample retail data...")
            return self._generate_sample_data()
    
    def _generate_sample_data(self, n_samples: int = 600) -> np.ndarray:
        """
        Generate sample retail data
        Features: Total Purchases, Avg Purchase Amount, Frequency, 
                  Recency (days), Product Categories Visited
        """
        np.random.seed(43)
        n1 = int(n_samples * 0.3)
        occasional = np.random.multivariate_normal(
            mean=[5, 30, 2, 90, 2],  # Purchases, Avg $, Frequency/month, Days ago, Categories
            cov=np.diag([4, 100, 1, 400, 1]),
            size=n1
        )
        n2 = int(n_samples * 0.35)
        regular = np.random.multivariate_normal(
            mean=[20, 50, 8, 30, 4],
            cov=np.diag([25, 225, 4, 225, 2]),
            size=n2
        )
        n3 = int(n_samples * 0.2)
        vip = np.random.multivariate_normal(
            mean=[50, 150, 20, 7, 6],
            cov=np.diag([100, 2500, 25, 16, 2]),
            size=n3
        )
        n4 = n_samples - n1 - n2 - n3
        churned = np.random.multivariate_normal(
            mean=[3, 25, 1, 180, 1],
            cov=np.diag([2, 64, 0.5, 900, 0.5]),
            size=n4
        )
        data = np.vstack([occasional, regular, vip, churned])
        mask = np.random.random(data.shape) < 0.03
        data[mask] = np.nan
        data = np.abs(data)
        data[:, 0] = np.clip(data[:, 0], 1, 100)  # Total purchases
        data[:, 1] = np.clip(data[:, 1], 10, 500)  # Avg amount
        data[:, 2] = np.clip(data[:, 2], 0, 30)  # Frequency
        data[:, 3] = np.clip(data[:, 3], 0, 365)  # Recency
        data[:, 4] = np.clip(data[:, 4], 1, 10)  # Categories
        
        self.logger.info(f"Generated {n_samples} sample customer records")
        df = pd.DataFrame(
            data,
            columns=['TotalPurchases', 'AvgAmount', 'Frequency', 'Recency', 'Categories']
        )
        
        os.makedirs(os.path.dirname(self.config['data_path']), exist_ok=True)
        df.to_csv(self.config['data_path'], index=False)
        self.logger.info(f"Saved sample data to {self.config['data_path']}")
        
        return data
    
    def preprocess_data(self, data: np.ndarray) -> np.ndarray:
        """
        Retail-specific preprocessing
        
        - Handle missing values (mean imputation)
        - MinMax scaling (preserves 0 values, important for retail)
        """
        self.logger.info("Preprocessing retail data...")
        imputer = SimpleImputer(strategy='mean')
        data = imputer.fit_transform(data)
        scaler = MinMaxScaler()
        data = scaler.fit_transform(data)
        
        self.logger.info("Scaled data to [0, 1] range")
        
        return data

app = Flask(__name__)

config_path = os.path.join(os.path.dirname(__file__), 'config.json')
retail_node = RetailNode(config_path)

@app.route('/health', methods=['GET'])
def health():
    return jsonify({
        'status': 'healthy',
        'node_id': retail_node.node_id,
        'node_type': retail_node.node_type
    })

@app.route('/status', methods=['GET'])
def status():
    return jsonify(retail_node.get_status())

@app.route('/local_cluster', methods=['POST'])
def local_cluster():
    try:
        results = retail_node.run_local_clustering()
        return jsonify({
            'status': 'success',
            'results': results
        })
    except Exception as e:
        retail_node.logger.error(f"Clustering failed: {e}")
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
        
        retail_node.run_local_clustering()
        response = retail_node.send_to_global_orchestrator(session_id)
        
        return jsonify({
            'status': 'success',
            'global_response': response
        })
        
    except Exception as e:
        retail_node.logger.error(f"Send to global failed: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

if __name__ == '__main__':
    print(f"""
╔═══════════════════════════════════════════════════════════════╗
║                                                               ║
║                     RETAIL NODE                               ║
║                                                               ║
║  Node ID: {retail_node.node_id:50s} ║
║  Type:    {retail_node.node_type:50s} ║
║  Port:    {retail_node.port:<50d} ║
║                                                               ║
╚═══════════════════════════════════════════════════════════════╝
    """)
    
    app.run(
        host='0.0.0.0',
        port=retail_node.port,
        debug=False
    )