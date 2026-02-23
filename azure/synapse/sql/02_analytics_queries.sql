-- ============================================================================
-- SYNAPSE SQL - Requêtes Analytiques pour Dashboard
-- ============================================================================
-- Équivalent Impala: requêtes massives sur données distribuées
-- Ces requêtes sont optimisées pour Synapse SQL Serverless
-- ============================================================================

USE FraudDetectionDB;
GO

-- ============================================================================
-- REQUÊTE 1: Dashboard Metrics - Vue globale
-- ============================================================================

SELECT
    'DASHBOARD METRICS' AS query_type,
    COUNT(*) AS total_transactions,
    COUNT(DISTINCT customerId) AS unique_customers,
    COUNT(DISTINCT merchantId) AS unique_merchants,
    CAST(SUM(amount) AS DECIMAL(18,2)) AS total_volume,
    CAST(AVG(amount) AS DECIMAL(10,2)) AS avg_transaction,
    CAST(MAX(amount) AS DECIMAL(10,2)) AS max_transaction,
    SUM(CASE WHEN isFraud = 1 THEN 1 ELSE 0 END) AS actual_frauds,
    SUM(CASE WHEN isFraudPredicted = 1 THEN 1 ELSE 0 END) AS predicted_frauds,
    SUM(CASE WHEN alertRequired = 1 THEN 1 ELSE 0 END) AS alerts_generated
FROM dbo.Transactions;
GO

-- ============================================================================
-- REQUÊTE 2: Distribution par niveau de risque
-- ============================================================================

SELECT
    riskLevel,
    COUNT(*) AS transaction_count,
    CAST(100.0 * COUNT(*) / SUM(COUNT(*)) OVER() AS DECIMAL(5,2)) AS percentage,
    CAST(SUM(amount) AS DECIMAL(18,2)) AS total_amount,
    CAST(AVG(fraudScore) AS DECIMAL(6,4)) AS avg_fraud_score,
    SUM(CASE WHEN isFraud = 1 THEN 1 ELSE 0 END) AS actual_frauds
FROM dbo.Transactions
GROUP BY riskLevel
ORDER BY
    CASE riskLevel
        WHEN 'CRITICAL' THEN 1
        WHEN 'HIGH' THEN 2
        WHEN 'MEDIUM' THEN 3
        ELSE 4
    END;
GO

-- ============================================================================
-- REQUÊTE 3: Analyse par catégorie marchand
-- ============================================================================

SELECT
    merchantCategory,
    COUNT(*) AS tx_count,
    CAST(SUM(amount) AS DECIMAL(18,2)) AS volume,
    CAST(AVG(amount) AS DECIMAL(10,2)) AS avg_amount,
    SUM(CASE WHEN isFraud = 1 THEN 1 ELSE 0 END) AS fraud_count,
    CAST(
        100.0 * SUM(CASE WHEN isFraud = 1 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0)
        AS DECIMAL(6,2)
    ) AS fraud_rate_pct,
    CAST(AVG(fraudScore) AS DECIMAL(6,4)) AS avg_score
FROM dbo.Transactions
GROUP BY merchantCategory
ORDER BY fraud_rate_pct DESC;
GO

-- ============================================================================
-- REQUÊTE 4: Analyse temporelle (par heure)
-- ============================================================================

SELECT
    hour,
    COUNT(*) AS tx_count,
    CAST(SUM(amount) AS DECIMAL(18,2)) AS volume,
    SUM(CASE WHEN isFraud = 1 THEN 1 ELSE 0 END) AS frauds,
    CAST(
        100.0 * SUM(CASE WHEN isFraud = 1 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0)
        AS DECIMAL(6,2)
    ) AS fraud_rate_pct,
    CASE
        WHEN hour BETWEEN 0 AND 5 THEN '🌙 Night (High Risk)'
        WHEN hour BETWEEN 6 AND 11 THEN '🌅 Morning'
        WHEN hour BETWEEN 12 AND 17 THEN '☀️ Afternoon'
        ELSE '🌆 Evening'
    END AS time_period
FROM dbo.Transactions
GROUP BY hour
ORDER BY hour;
GO

-- ============================================================================
-- REQUÊTE 5: Top clients à risque (CTE - Common Table Expression)
-- ============================================================================

WITH CustomerRiskStats AS (
    SELECT
        customerId,
        COUNT(*) AS total_transactions,
        CAST(SUM(amount) AS DECIMAL(18,2)) AS total_spent,
        CAST(AVG(amount) AS DECIMAL(10,2)) AS avg_transaction,
        CAST(AVG(fraudScore) AS DECIMAL(6,4)) AS avg_fraud_score,
        SUM(CASE WHEN isFraud = 1 THEN 1 ELSE 0 END) AS fraud_count,
        SUM(CASE WHEN alertRequired = 1 THEN 1 ELSE 0 END) AS alert_count
    FROM dbo.Transactions
    GROUP BY customerId
),
RankedCustomers AS (
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY avg_fraud_score DESC) AS risk_rank
    FROM CustomerRiskStats
    WHERE total_transactions >= 3  -- Au moins 3 transactions
)
SELECT TOP 20
    customerId,
    total_transactions,
    total_spent,
    avg_transaction,
    avg_fraud_score,
    fraud_count,
    alert_count,
    risk_rank
FROM RankedCustomers
ORDER BY risk_rank;
GO

-- ============================================================================
-- REQUÊTE 6: Matrice de confusion
-- ============================================================================

SELECT
    'CONFUSION MATRIX' AS metric_type,
    SUM(CASE WHEN isFraudPredicted = 1 AND isFraud = 1 THEN 1 ELSE 0 END) AS TP,
    SUM(CASE WHEN isFraudPredicted = 1 AND isFraud = 0 THEN 1 ELSE 0 END) AS FP,
    SUM(CASE WHEN isFraudPredicted = 0 AND isFraud = 1 THEN 1 ELSE 0 END) AS FN,
    SUM(CASE WHEN isFraudPredicted = 0 AND isFraud = 0 THEN 1 ELSE 0 END) AS TN,
    -- Precision = TP / (TP + FP)
    CAST(
        1.0 * SUM(CASE WHEN isFraudPredicted = 1 AND isFraud = 1 THEN 1 ELSE 0 END) /
        NULLIF(SUM(CASE WHEN isFraudPredicted = 1 THEN 1 ELSE 0 END), 0)
        AS DECIMAL(6,4)
    ) AS precision_score,
    -- Recall = TP / (TP + FN)
    CAST(
        1.0 * SUM(CASE WHEN isFraudPredicted = 1 AND isFraud = 1 THEN 1 ELSE 0 END) /
        NULLIF(SUM(CASE WHEN isFraud = 1 THEN 1 ELSE 0 END), 0)
        AS DECIMAL(6,4)
    ) AS recall_score
FROM dbo.Transactions;
GO

-- ============================================================================
-- REQUÊTE 7: Window Functions - Vélocité par client
-- ============================================================================

WITH TransactionSequence AS (
    SELECT
        customerId,
        transactionId,
        amount,
        [timestamp],
        ROW_NUMBER() OVER (PARTITION BY customerId ORDER BY [timestamp]) AS tx_sequence,
        SUM(amount) OVER (PARTITION BY customerId ORDER BY [timestamp] ROWS UNBOUNDED PRECEDING) AS cumulative_amount,
        COUNT(*) OVER (PARTITION BY customerId) AS total_customer_tx,
        LAG([timestamp]) OVER (PARTITION BY customerId ORDER BY [timestamp]) AS prev_timestamp
    FROM dbo.Transactions
)
SELECT TOP 50
    customerId,
    transactionId,
    CAST(amount AS DECIMAL(10,2)) AS amount,
    tx_sequence,
    CAST(cumulative_amount AS DECIMAL(18,2)) AS cumulative_amount,
    total_customer_tx,
    -- Temps depuis dernière transaction (en minutes)
    CASE
        WHEN prev_timestamp IS NOT NULL
        THEN ([timestamp] - prev_timestamp) / 60000
        ELSE NULL
    END AS minutes_since_last_tx
FROM TransactionSequence
WHERE total_customer_tx >= 5  -- Clients avec au moins 5 transactions
ORDER BY customerId, tx_sequence;
GO

-- ============================================================================
-- REQUÊTE 8: Analyse géographique
-- ============================================================================

SELECT
    location,
    country,
    COUNT(*) AS tx_count,
    CAST(SUM(amount) AS DECIMAL(18,2)) AS volume,
    SUM(CASE WHEN isInternational = 1 THEN 1 ELSE 0 END) AS international_tx,
    SUM(CASE WHEN isFraud = 1 THEN 1 ELSE 0 END) AS frauds,
    CAST(
        100.0 * SUM(CASE WHEN isFraud = 1 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0)
        AS DECIMAL(6,2)
    ) AS fraud_rate_pct,
    CAST(AVG(fraudScore) AS DECIMAL(6,4)) AS avg_fraud_score
FROM dbo.Transactions
GROUP BY location, country
ORDER BY fraud_rate_pct DESC;
GO

-- ============================================================================
-- REQUÊTE 9: Analyse des alertes
-- ============================================================================

SELECT
    riskLevel,
    COUNT(*) AS alert_count,
    CAST(SUM(amount) AS DECIMAL(18,2)) AS total_amount,
    CAST(AVG(amount) AS DECIMAL(10,2)) AS avg_amount,
    CAST(AVG(fraudScore) AS DECIMAL(6,4)) AS avg_fraud_score,
    COUNT(DISTINCT customerId) AS unique_customers,
    COUNT(DISTINCT merchantId) AS unique_merchants
FROM dbo.FraudAlerts
GROUP BY riskLevel
ORDER BY
    CASE riskLevel WHEN 'CRITICAL' THEN 1 ELSE 2 END;
GO

-- ============================================================================
-- REQUÊTE 10: Trend Analysis - Évolution quotidienne
-- ============================================================================

SELECT
    transaction_date,
    COUNT(*) AS daily_transactions,
    CAST(SUM(amount) AS DECIMAL(18,2)) AS daily_volume,
    SUM(CASE WHEN isFraud = 1 THEN 1 ELSE 0 END) AS daily_frauds,
    SUM(CASE WHEN alertRequired = 1 THEN 1 ELSE 0 END) AS daily_alerts,
    CAST(AVG(fraudScore) AS DECIMAL(6,4)) AS avg_fraud_score,
    -- Moving average (7 derniers jours)
    CAST(
        AVG(COUNT(*)) OVER (ORDER BY transaction_date ROWS 6 PRECEDING)
        AS DECIMAL(10,0)
    ) AS moving_avg_7d_tx
FROM dbo.Transactions
GROUP BY transaction_date
ORDER BY transaction_date DESC;
GO

PRINT '✅ Requêtes analytiques exécutées avec succès!';
GO
