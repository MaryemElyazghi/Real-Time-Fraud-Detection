-- =============================================================================
-- Fraud Detection - Impala Tables
-- =============================================================================
-- Ce script crée les tables externes Impala pour l'analyse des fraudes
-- Les tables sont mappées sur les données HDFS en format Parquet
-- =============================================================================

-- Créer la base de données
CREATE DATABASE IF NOT EXISTS fraud_detection
COMMENT 'Base de données pour le système de détection de fraude'
LOCATION '/fraud-detection/warehouse';

USE fraud_detection;

-- =============================================================================
-- TABLE: transactions
-- Description: Table principale des transactions financières
-- Partitionnée par date et niveau de risque pour des requêtes optimisées
-- =============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS transactions (
    transaction_id STRING COMMENT 'Identifiant unique de la transaction',
    customer_id STRING COMMENT 'Identifiant du client',
    merchant_id STRING COMMENT 'Identifiant du marchand',
    amount DOUBLE COMMENT 'Montant de la transaction',
    currency STRING COMMENT 'Devise (EUR, USD, etc.)',
    transaction_type STRING COMMENT 'Type: PURCHASE, WITHDRAWAL, TRANSFER',
    channel STRING COMMENT 'Canal: ONLINE, POS, ATM, MOBILE',
    location STRING COMMENT 'Localisation géographique',
    device_id STRING COMMENT 'Identifiant de l appareil',
    ip_address STRING COMMENT 'Adresse IP pour transactions en ligne',
    timestamp_ms BIGINT COMMENT 'Timestamp Unix en millisecondes',
    card_type STRING COMMENT 'Type de carte: DEBIT, CREDIT',
    is_international BOOLEAN COMMENT 'Transaction internationale',
    merchant_category STRING COMMENT 'Catégorie du marchand (MCC)',
    previous_balance DOUBLE COMMENT 'Solde avant transaction',
    fraud_score DOUBLE COMMENT 'Score de fraude calculé (0-1)',
    is_fraud_predicted BOOLEAN COMMENT 'Prédiction de fraude'
)
PARTITIONED BY (
    risk_level STRING COMMENT 'Niveau de risque: LOW, MEDIUM, HIGH, CRITICAL',
    transaction_date DATE COMMENT 'Date de la transaction'
)
STORED AS PARQUET
LOCATION '/fraud-detection/processed/transactions'
TBLPROPERTIES (
    'parquet.compression'='SNAPPY'
);

-- Récupérer les partitions existantes
ALTER TABLE transactions RECOVER PARTITIONS;

-- =============================================================================
-- TABLE: fraud_predictions
-- Description: Résultats des prédictions du modèle ML
-- =============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS fraud_predictions (
    prediction_id STRING COMMENT 'Identifiant unique de la prédiction',
    transaction_id STRING COMMENT 'Référence à la transaction',
    customer_id STRING COMMENT 'Identifiant du client',
    fraud_probability DOUBLE COMMENT 'Probabilité de fraude (0-1)',
    is_fraud BOOLEAN COMMENT 'Décision finale de fraude',
    risk_level STRING COMMENT 'Niveau de risque: LOW, MEDIUM, HIGH, CRITICAL',
    alert_reasons ARRAY<STRING> COMMENT 'Raisons de l alerte',
    model_version STRING COMMENT 'Version du modèle utilisé',
    processed_at TIMESTAMP COMMENT 'Timestamp du traitement'
)
PARTITIONED BY (
    prediction_date DATE COMMENT 'Date de la prédiction'
)
STORED AS PARQUET
LOCATION '/fraud-detection/processed/predictions'
TBLPROPERTIES (
    'parquet.compression'='SNAPPY'
);

ALTER TABLE fraud_predictions RECOVER PARTITIONS;

-- =============================================================================
-- TABLE: customer_profiles
-- Description: Profils clients avec historique agrégé
-- =============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS customer_profiles (
    customer_id STRING COMMENT 'Identifiant du client',
    avg_transaction_amount DOUBLE COMMENT 'Montant moyen des transactions',
    std_transaction_amount DOUBLE COMMENT 'Écart-type du montant',
    max_transaction_amount DOUBLE COMMENT 'Montant maximum historique',
    total_transactions BIGINT COMMENT 'Nombre total de transactions',
    avg_transactions_per_day DOUBLE COMMENT 'Moyenne de transactions par jour',
    fraud_count INT COMMENT 'Nombre de fraudes détectées',
    fraud_rate DOUBLE COMMENT 'Taux de fraude historique',
    preferred_locations ARRAY<STRING> COMMENT 'Localisations habituelles',
    preferred_merchants ARRAY<STRING> COMMENT 'Marchands habituels',
    last_transaction_time TIMESTAMP COMMENT 'Dernière transaction',
    profile_updated_at TIMESTAMP COMMENT 'Dernière mise à jour du profil'
)
STORED AS PARQUET
LOCATION '/fraud-detection/profiles/customers'
TBLPROPERTIES (
    'parquet.compression'='SNAPPY'
);

-- =============================================================================
-- TABLE: merchant_risk_scores
-- Description: Scores de risque par marchand
-- =============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS merchant_risk_scores (
    merchant_id STRING COMMENT 'Identifiant du marchand',
    merchant_category STRING COMMENT 'Catégorie du marchand',
    total_transactions BIGINT COMMENT 'Nombre total de transactions',
    total_volume DOUBLE COMMENT 'Volume total traité',
    fraud_count INT COMMENT 'Nombre de fraudes',
    fraud_volume DOUBLE COMMENT 'Volume de fraudes',
    fraud_rate DOUBLE COMMENT 'Taux de fraude',
    risk_score DOUBLE COMMENT 'Score de risque (0-1)',
    risk_category STRING COMMENT 'Catégorie de risque',
    last_updated TIMESTAMP COMMENT 'Dernière mise à jour'
)
STORED AS PARQUET
LOCATION '/fraud-detection/analytics/merchant_risk';

-- =============================================================================
-- TABLE: fraud_alerts
-- Description: Alertes de fraude générées
-- =============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS fraud_alerts (
    alert_id STRING COMMENT 'Identifiant unique de l alerte',
    transaction_id STRING COMMENT 'Transaction associée',
    customer_id STRING COMMENT 'Client concerné',
    alert_level STRING COMMENT 'Niveau: WARNING, CRITICAL',
    alert_type STRING COMMENT 'Type d alerte',
    description STRING COMMENT 'Description de l alerte',
    fraud_score DOUBLE COMMENT 'Score de fraude',
    amount DOUBLE COMMENT 'Montant de la transaction',
    status STRING COMMENT 'Statut: OPEN, INVESTIGATING, CLOSED',
    created_at TIMESTAMP COMMENT 'Date de création',
    resolved_at TIMESTAMP COMMENT 'Date de résolution',
    resolved_by STRING COMMENT 'Analyste qui a résolu'
)
PARTITIONED BY (
    alert_date DATE COMMENT 'Date de l alerte'
)
STORED AS PARQUET
LOCATION '/fraud-detection/alerts';

ALTER TABLE fraud_alerts RECOVER PARTITIONS;

-- =============================================================================
-- TABLE: hourly_metrics
-- Description: Métriques agrégées par heure pour dashboard
-- =============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS hourly_metrics (
    metric_hour TIMESTAMP COMMENT 'Heure de la métrique',
    total_transactions BIGINT COMMENT 'Nombre de transactions',
    total_volume DOUBLE COMMENT 'Volume total',
    avg_amount DOUBLE COMMENT 'Montant moyen',
    fraud_count INT COMMENT 'Nombre de fraudes détectées',
    fraud_volume DOUBLE COMMENT 'Volume de fraudes',
    fraud_rate DOUBLE COMMENT 'Taux de fraude',
    critical_alerts INT COMMENT 'Alertes critiques',
    high_alerts INT COMMENT 'Alertes hautes',
    avg_latency_ms DOUBLE COMMENT 'Latence moyenne de traitement',
    unique_customers INT COMMENT 'Clients uniques',
    unique_merchants INT COMMENT 'Marchands uniques'
)
STORED AS PARQUET
LOCATION '/fraud-detection/metrics/hourly';

-- =============================================================================
-- VUES ANALYTIQUES
-- =============================================================================

-- Vue: Résumé des fraudes du jour
CREATE VIEW IF NOT EXISTS daily_fraud_summary AS
SELECT
    transaction_date,
    COUNT(*) as total_transactions,
    SUM(amount) as total_volume,
    SUM(CASE WHEN is_fraud_predicted THEN 1 ELSE 0 END) as fraud_count,
    SUM(CASE WHEN is_fraud_predicted THEN amount ELSE 0 END) as fraud_volume,
    AVG(fraud_score) as avg_fraud_score,
    COUNT(DISTINCT customer_id) as unique_customers
FROM transactions
WHERE transaction_date >= DATE_SUB(CURRENT_DATE(), 7)
GROUP BY transaction_date
ORDER BY transaction_date DESC;

-- Vue: Top clients à risque
CREATE VIEW IF NOT EXISTS high_risk_customers AS
SELECT
    t.customer_id,
    COUNT(*) as transaction_count,
    SUM(t.amount) as total_amount,
    AVG(t.fraud_score) as avg_fraud_score,
    SUM(CASE WHEN t.is_fraud_predicted THEN 1 ELSE 0 END) as fraud_predictions,
    cp.fraud_count as historical_frauds,
    cp.fraud_rate as historical_fraud_rate
FROM transactions t
LEFT JOIN customer_profiles cp ON t.customer_id = cp.customer_id
WHERE t.transaction_date >= DATE_SUB(CURRENT_DATE(), 30)
GROUP BY t.customer_id, cp.fraud_count, cp.fraud_rate
HAVING AVG(t.fraud_score) > 0.5 OR cp.fraud_rate > 0.01
ORDER BY avg_fraud_score DESC
LIMIT 100;

-- Vue: Performance du modèle par jour
CREATE VIEW IF NOT EXISTS model_performance_daily AS
SELECT
    prediction_date,
    COUNT(*) as total_predictions,
    SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) as fraud_predictions,
    AVG(fraud_probability) as avg_probability,
    MAX(fraud_probability) as max_probability,
    model_version
FROM fraud_predictions
WHERE prediction_date >= DATE_SUB(CURRENT_DATE(), 30)
GROUP BY prediction_date, model_version
ORDER BY prediction_date DESC;

-- Vue: Analyse par catégorie de marchand
CREATE VIEW IF NOT EXISTS merchant_category_analysis AS
SELECT
    merchant_category,
    risk_level,
    COUNT(*) as transaction_count,
    SUM(amount) as total_volume,
    AVG(amount) as avg_amount,
    AVG(fraud_score) as avg_fraud_score,
    SUM(CASE WHEN is_fraud_predicted THEN 1 ELSE 0 END) as fraud_count
FROM transactions
WHERE transaction_date >= DATE_SUB(CURRENT_DATE(), 7)
GROUP BY merchant_category, risk_level
ORDER BY merchant_category, risk_level;

-- =============================================================================
-- COMPUTE STATISTICS (pour optimiser les requêtes)
-- =============================================================================

COMPUTE STATS transactions;
COMPUTE STATS fraud_predictions;
COMPUTE STATS customer_profiles;
COMPUTE STATS merchant_risk_scores;
COMPUTE STATS fraud_alerts;
