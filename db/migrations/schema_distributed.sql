CREATE DATABASE IF NOT EXISTS distributed_clustering
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE distributed_clustering;

CREATE TABLE IF NOT EXISTS distributed_nodes (
    id                          INT AUTO_INCREMENT PRIMARY KEY,
    node_id                     VARCHAR(255) UNIQUE NOT NULL,
    node_type                   VARCHAR(50)  NOT NULL,
    location                    VARCHAR(255) DEFAULT 'localhost',
    config_json                 JSON,
    status                      VARCHAR(20)  DEFAULT 'active',
    last_heartbeat              TIMESTAMP    NULL,
    total_analyses              INT          DEFAULT 0,
    total_data_points_processed BIGINT       DEFAULT 0,
    created_at                  TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    updated_at                  TIMESTAMP    DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    INDEX idx_node_id (node_id),
    INDEX idx_status  (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS global_analyses (
    id                       INT AUTO_INCREMENT PRIMARY KEY,
    session_id               VARCHAR(255) UNIQUE NOT NULL,
    total_nodes              INT          NOT NULL,
    total_data_points        BIGINT       NOT NULL,
    best_algorithm           VARCHAR(50),
    best_algorithm_score     FLOAT,
    global_clusters          JSON,
    algorithm_aggregations   JSON,
    ensemble_analysis        JSON,
    cross_org_insights       JSON,
    pca_visualization        JSON         DEFAULT NULL,
    execution_time_total_ms  INT,
    status                   VARCHAR(20)  DEFAULT 'pending',
    error_message            TEXT         NULL,
    created_at               TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    completed_at             TIMESTAMP    NULL,

    INDEX idx_session (session_id),
    INDEX idx_status  (status),
    INDEX idx_created (created_at DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS node_local_results (
    id                   INT AUTO_INCREMENT PRIMARY KEY,
    node_id              VARCHAR(255) NOT NULL,
    session_id           VARCHAR(255) NOT NULL,
    algorithm            VARCHAR(50)  NOT NULL,
    n_local_clusters     INT,
    silhouette_score     FLOAT,
    davies_bouldin_score FLOAT        DEFAULT NULL,
    execution_time_ms    INT          DEFAULT 0,
    cluster_centers      JSON,
    cluster_sizes        JSON,
    cluster_stds         JSON,
    data_summary         JSON,
    created_at           TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,

    INDEX idx_session      (session_id),
    INDEX idx_node_session (node_id, session_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS analysis_node_participation (
    id                       INT AUTO_INCREMENT PRIMARY KEY,
    global_analysis_id       INT          NOT NULL,
    node_id                  VARCHAR(255) NOT NULL,
    contribution_weight      FLOAT        DEFAULT 0.0,
    data_points_contributed  INT          DEFAULT 0,
    best_local_algorithm     VARCHAR(50),
    best_local_score         FLOAT,
    created_at               TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (global_analysis_id) REFERENCES global_analyses(id) ON DELETE CASCADE,
    INDEX idx_analysis (global_analysis_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS node_performance_metrics (
    id                    INT AUTO_INCREMENT PRIMARY KEY,
    node_id               VARCHAR(255) NOT NULL,
    avg_silhouette_7d     FLOAT,
    total_analyses_7d     INT,
    avg_execution_time_ms INT,
    cpu_usage_percent     FLOAT        DEFAULT NULL,
    memory_usage_mb       FLOAT        DEFAULT NULL,
    recorded_at           TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,

    INDEX idx_node_time (node_id, recorded_at DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS datasets (
    id                INT AUTO_INCREMENT PRIMARY KEY,
    name              VARCHAR(255) NOT NULL,
    original_filename VARCHAR(255) NOT NULL,
    file_type         VARCHAR(10)  NOT NULL COMMENT 'csv, xlsx, json',
    file_data         LONGBLOB     NOT NULL,
    columns_info      JSON         COMMENT 'column names, types, numeric columns',
    row_count         INT          DEFAULT 0,
    file_size_bytes   BIGINT       DEFAULT 0,
    is_default        TINYINT(1)   DEFAULT 0 COMMENT '1 = auto-seeded, cannot delete',
    description       TEXT,
    uploaded_at       TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,

    INDEX idx_name (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS node_dataset_assignments (
    id          INT AUTO_INCREMENT PRIMARY KEY,
    node_id     VARCHAR(255) NOT NULL,
    dataset_id  INT          NOT NULL,
    is_active   TINYINT(1)   DEFAULT 0 COMMENT '1 = currently used by the node',
    assigned_at TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (dataset_id) REFERENCES datasets(id) ON DELETE CASCADE,
    UNIQUE KEY uq_node_dataset (node_id, dataset_id),
    INDEX idx_node_active (node_id, is_active)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT IGNORE INTO distributed_nodes (node_id, node_type, location, status) VALUES
    ('medical_bucharest_01', 'healthcare', 'Bucharest Medical Center', 'active'),
    ('retail_bucharest_01',  'retail',     'Bucharest Retail Hub',     'active'),
    ('iot_bucharest_01',     'iot',        'Bucharest IoT Network',    'active');

SELECT 'Schema created successfully — 7 tables + seed data' AS status;