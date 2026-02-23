-- =============================================================================
-- Fraud Detection - Requêtes Impala Avancées
-- =============================================================================
-- Requêtes optimisées pour l'analyse massive des données de fraude
-- Utilise les fonctionnalités de SQL distribué d'Impala
-- =============================================================================

USE fraud_detection;

-- =============================================================================
-- 1. REQUÊTES TEMPS RÉEL DASHBOARD
-- =============================================================================

-- Métriques en temps réel (dernière heure)
SELECT
    DATE_TRUNC('minute', FROM_UNIXTIME(timestamp_ms / 1000)) as minute,
    COUNT(*) as transactions,
    SUM(amount) as volume,
    SUM(CASE WHEN is_fraud_predicted THEN 1 ELSE 0 END) as frauds,
    AVG(fraud_score) as avg_score
FROM transactions
WHERE timestamp_ms > (UNIX_TIMESTAMP() - 3600) * 1000
GROUP BY DATE_TRUNC('minute', FROM_UNIXTIME(timestamp_ms / 1000))
ORDER BY minute DESC;

-- Alertes actives par niveau
SELECT
    alert_level,
    COUNT(*) as alert_count,
    AVG(fraud_score) as avg_score,
    SUM(amount) as total_amount
FROM fraud_alerts
WHERE status = 'OPEN'
  AND alert_date >= DATE_SUB(CURRENT_DATE(), 1)
GROUP BY alert_level
ORDER BY
    CASE alert_level
        WHEN 'CRITICAL' THEN 1
        WHEN 'WARNING' THEN 2
    END;

-- =============================================================================
-- 2. ANALYSE DE VÉLOCITÉ
-- =============================================================================

-- Clients avec vélocité anormale (>10 transactions/heure)
WITH customer_velocity AS (
    SELECT
        customer_id,
        DATE_TRUNC('hour', FROM_UNIXTIME(timestamp_ms / 1000)) as hour,
        COUNT(*) as tx_count,
        SUM(amount) as total_amount,
        COUNT(DISTINCT location) as unique_locations
    FROM transactions
    WHERE transaction_date = CURRENT_DATE()
    GROUP BY customer_id, DATE_TRUNC('hour', FROM_UNIXTIME(timestamp_ms / 1000))
)
SELECT
    cv.*,
    cp.avg_transactions_per_day,
    cv.tx_count / NULLIF(cp.avg_transactions_per_day, 0) as velocity_ratio
FROM customer_velocity cv
LEFT JOIN customer_profiles cp ON cv.customer_id = cp.customer_id
WHERE cv.tx_count > 10 OR cv.unique_locations > 3
ORDER BY cv.tx_count DESC
LIMIT 100;

-- =============================================================================
-- 3. DÉTECTION DE PATTERNS
-- =============================================================================

-- Pattern: Card Testing (petits montants répétés)
WITH small_transactions AS (
    SELECT
        customer_id,
        DATE_TRUNC('hour', FROM_UNIXTIME(timestamp_ms / 1000)) as hour,
        COUNT(*) as small_tx_count,
        SUM(amount) as total_small_amount
    FROM transactions
    WHERE amount < 5
      AND transaction_date >= DATE_SUB(CURRENT_DATE(), 1)
    GROUP BY customer_id, DATE_TRUNC('hour', FROM_UNIXTIME(timestamp_ms / 1000))
    HAVING COUNT(*) >= 3
)
SELECT
    st.*,
    t.transaction_id,
    t.merchant_category,
    t.channel
FROM small_transactions st
JOIN transactions t ON st.customer_id = t.customer_id
WHERE t.amount < 5
  AND DATE_TRUNC('hour', FROM_UNIXTIME(t.timestamp_ms / 1000)) = st.hour
ORDER BY st.customer_id, t.timestamp_ms;

-- Pattern: Voyage impossible
WITH transaction_sequence AS (
    SELECT
        customer_id,
        transaction_id,
        location,
        timestamp_ms,
        LAG(location) OVER (PARTITION BY customer_id ORDER BY timestamp_ms) as prev_location,
        LAG(timestamp_ms) OVER (PARTITION BY customer_id ORDER BY timestamp_ms) as prev_timestamp,
        amount
    FROM transactions
    WHERE transaction_date >= DATE_SUB(CURRENT_DATE(), 1)
)
SELECT
    customer_id,
    transaction_id,
    prev_location,
    location as current_location,
    (timestamp_ms - prev_timestamp) / 1000 / 60 as minutes_between,
    amount
FROM transaction_sequence
WHERE prev_location IS NOT NULL
  AND prev_location != location
  AND (timestamp_ms - prev_timestamp) / 1000 / 60 < 30
ORDER BY minutes_between ASC
LIMIT 50;

-- =============================================================================
-- 4. ANALYSE PAR SEGMENT
-- =============================================================================

-- Analyse par catégorie de marchand et canal
SELECT
    merchant_category,
    channel,
    risk_level,
    COUNT(*) as transaction_count,
    SUM(amount) as total_volume,
    AVG(amount) as avg_amount,
    PERCENTILE_APPROX(amount, 0.5) as median_amount,
    PERCENTILE_APPROX(amount, 0.95) as p95_amount,
    SUM(CASE WHEN is_fraud_predicted THEN 1 ELSE 0 END) as fraud_count,
    AVG(fraud_score) as avg_fraud_score
FROM transactions
WHERE transaction_date >= DATE_SUB(CURRENT_DATE(), 7)
GROUP BY merchant_category, channel, risk_level
ORDER BY merchant_category, channel, risk_level;

-- Top marchands par taux de fraude
SELECT
    merchant_id,
    merchant_category,
    COUNT(*) as total_transactions,
    SUM(amount) as total_volume,
    SUM(CASE WHEN is_fraud_predicted THEN 1 ELSE 0 END) as fraud_count,
    SUM(CASE WHEN is_fraud_predicted THEN amount ELSE 0 END) as fraud_volume,
    CAST(SUM(CASE WHEN is_fraud_predicted THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) * 100 as fraud_rate_percent
FROM transactions
WHERE transaction_date >= DATE_SUB(CURRENT_DATE(), 30)
GROUP BY merchant_id, merchant_category
HAVING COUNT(*) >= 100
ORDER BY fraud_rate_percent DESC
LIMIT 50;

-- =============================================================================
-- 5. ANALYSE TEMPORELLE
-- =============================================================================

-- Distribution horaire des fraudes
SELECT
    HOUR(FROM_UNIXTIME(timestamp_ms / 1000)) as hour_of_day,
    COUNT(*) as total_transactions,
    SUM(CASE WHEN is_fraud_predicted THEN 1 ELSE 0 END) as fraud_count,
    AVG(CASE WHEN is_fraud_predicted THEN 1.0 ELSE 0.0 END) * 100 as fraud_rate_percent,
    SUM(amount) as total_volume,
    SUM(CASE WHEN is_fraud_predicted THEN amount ELSE 0 END) as fraud_volume
FROM transactions
WHERE transaction_date >= DATE_SUB(CURRENT_DATE(), 30)
GROUP BY HOUR(FROM_UNIXTIME(timestamp_ms / 1000))
ORDER BY hour_of_day;

-- Tendance journalière avec comparaison semaine précédente
SELECT
    t1.transaction_date as date,
    t1.total_transactions as current_transactions,
    t2.total_transactions as prev_week_transactions,
    t1.fraud_count as current_frauds,
    t2.fraud_count as prev_week_frauds,
    (t1.total_transactions - t2.total_transactions) / NULLIF(t2.total_transactions, 0) * 100 as tx_change_percent,
    (t1.fraud_count - t2.fraud_count) / NULLIF(t2.fraud_count, 0) * 100 as fraud_change_percent
FROM (
    SELECT
        transaction_date,
        COUNT(*) as total_transactions,
        SUM(CASE WHEN is_fraud_predicted THEN 1 ELSE 0 END) as fraud_count
    FROM transactions
    WHERE transaction_date >= DATE_SUB(CURRENT_DATE(), 14)
    GROUP BY transaction_date
) t1
LEFT JOIN (
    SELECT
        transaction_date,
        COUNT(*) as total_transactions,
        SUM(CASE WHEN is_fraud_predicted THEN 1 ELSE 0 END) as fraud_count
    FROM transactions
    WHERE transaction_date >= DATE_SUB(CURRENT_DATE(), 21)
      AND transaction_date < DATE_SUB(CURRENT_DATE(), 7)
    GROUP BY transaction_date
) t2 ON t1.transaction_date = DATE_ADD(t2.transaction_date, 7)
ORDER BY t1.transaction_date;

-- =============================================================================
-- 6. REQUÊTES AVEC CUBE/ROLLUP (analyse multidimensionnelle)
-- =============================================================================

-- Analyse CUBE par catégorie, canal et risque
SELECT
    COALESCE(merchant_category, 'ALL') as merchant_category,
    COALESCE(channel, 'ALL') as channel,
    COALESCE(risk_level, 'ALL') as risk_level,
    COUNT(*) as transaction_count,
    SUM(amount) as total_volume,
    AVG(fraud_score) as avg_fraud_score
FROM transactions
WHERE transaction_date >= DATE_SUB(CURRENT_DATE(), 7)
GROUP BY CUBE (merchant_category, channel, risk_level)
ORDER BY
    GROUPING(merchant_category),
    GROUPING(channel),
    GROUPING(risk_level),
    transaction_count DESC;

-- =============================================================================
-- 7. CORRÉLATION ET STATISTIQUES AVANCÉES
-- =============================================================================

-- Corrélation entre features et fraude
SELECT
    'amount' as feature,
    CORR(amount, CASE WHEN is_fraud_predicted THEN 1.0 ELSE 0.0 END) as correlation
FROM transactions
WHERE transaction_date >= DATE_SUB(CURRENT_DATE(), 30)
UNION ALL
SELECT
    'is_international',
    CORR(CASE WHEN is_international THEN 1.0 ELSE 0.0 END,
         CASE WHEN is_fraud_predicted THEN 1.0 ELSE 0.0 END)
FROM transactions
WHERE transaction_date >= DATE_SUB(CURRENT_DATE(), 30)
UNION ALL
SELECT
    'previous_balance',
    CORR(previous_balance, CASE WHEN is_fraud_predicted THEN 1.0 ELSE 0.0 END)
FROM transactions
WHERE transaction_date >= DATE_SUB(CURRENT_DATE(), 30);

-- Distribution des scores de fraude par décile
SELECT
    NTILE(10) OVER (ORDER BY fraud_score) as decile,
    MIN(fraud_score) as min_score,
    MAX(fraud_score) as max_score,
    COUNT(*) as transaction_count,
    SUM(CASE WHEN is_fraud_predicted THEN 1 ELSE 0 END) as actual_frauds
FROM transactions
WHERE transaction_date >= DATE_SUB(CURRENT_DATE(), 7)
GROUP BY NTILE(10) OVER (ORDER BY fraud_score)
ORDER BY decile;

-- =============================================================================
-- 8. PERFORMANCE DU MODÈLE
-- =============================================================================

-- Matrice de confusion par jour
SELECT
    prediction_date,
    SUM(CASE WHEN is_fraud AND fraud_probability >= 0.7 THEN 1 ELSE 0 END) as true_positives,
    SUM(CASE WHEN NOT is_fraud AND fraud_probability >= 0.7 THEN 1 ELSE 0 END) as false_positives,
    SUM(CASE WHEN is_fraud AND fraud_probability < 0.7 THEN 1 ELSE 0 END) as false_negatives,
    SUM(CASE WHEN NOT is_fraud AND fraud_probability < 0.7 THEN 1 ELSE 0 END) as true_negatives
FROM fraud_predictions
WHERE prediction_date >= DATE_SUB(CURRENT_DATE(), 7)
GROUP BY prediction_date
ORDER BY prediction_date;

-- Courbe precision-recall par seuil
WITH thresholds AS (
    SELECT threshold
    FROM (VALUES (0.1), (0.2), (0.3), (0.4), (0.5), (0.6), (0.7), (0.8), (0.9)) t(threshold)
)
SELECT
    th.threshold,
    COUNT(*) as total,
    SUM(CASE WHEN fp.fraud_probability >= th.threshold THEN 1 ELSE 0 END) as predicted_positive,
    SUM(CASE WHEN fp.is_fraud AND fp.fraud_probability >= th.threshold THEN 1 ELSE 0 END) as true_positive,
    CAST(SUM(CASE WHEN fp.is_fraud AND fp.fraud_probability >= th.threshold THEN 1 ELSE 0 END) AS DOUBLE) /
        NULLIF(SUM(CASE WHEN fp.fraud_probability >= th.threshold THEN 1 ELSE 0 END), 0) as precision,
    CAST(SUM(CASE WHEN fp.is_fraud AND fp.fraud_probability >= th.threshold THEN 1 ELSE 0 END) AS DOUBLE) /
        NULLIF(SUM(CASE WHEN fp.is_fraud THEN 1 ELSE 0 END), 0) as recall
FROM fraud_predictions fp
CROSS JOIN thresholds th
WHERE fp.prediction_date >= DATE_SUB(CURRENT_DATE(), 7)
GROUP BY th.threshold
ORDER BY th.threshold;

-- =============================================================================
-- 9. REQUÊTES POUR ALERTES
-- =============================================================================

-- Transactions nécessitant une investigation immédiate
SELECT
    t.transaction_id,
    t.customer_id,
    t.amount,
    t.merchant_category,
    t.location,
    t.fraud_score,
    t.risk_level,
    cp.fraud_count as historical_frauds,
    cp.avg_transaction_amount,
    t.amount / NULLIF(cp.avg_transaction_amount, 0) as amount_ratio
FROM transactions t
LEFT JOIN customer_profiles cp ON t.customer_id = cp.customer_id
WHERE t.transaction_date = CURRENT_DATE()
  AND t.risk_level IN ('CRITICAL', 'HIGH')
  AND (
      t.amount > 5000
      OR t.amount / NULLIF(cp.avg_transaction_amount, 0) > 5
      OR cp.fraud_count > 0
  )
ORDER BY t.fraud_score DESC
LIMIT 100;

-- Résumé pour rapport de fin de journée
SELECT
    'Aujourd hui' as period,
    COUNT(*) as total_transactions,
    SUM(amount) as total_volume,
    SUM(CASE WHEN is_fraud_predicted THEN 1 ELSE 0 END) as frauds_detected,
    SUM(CASE WHEN is_fraud_predicted THEN amount ELSE 0 END) as fraud_volume_blocked,
    AVG(fraud_score) as avg_fraud_score,
    COUNT(DISTINCT customer_id) as unique_customers,
    SUM(CASE WHEN risk_level = 'CRITICAL' THEN 1 ELSE 0 END) as critical_transactions
FROM transactions
WHERE transaction_date = CURRENT_DATE();
