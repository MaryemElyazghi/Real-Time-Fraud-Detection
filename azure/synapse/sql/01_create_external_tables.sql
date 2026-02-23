-- ============================================================================
-- SYNAPSE SQL SERVERLESS - External Tables sur Data Lake
-- ============================================================================
-- Ces tables lisent directement les fichiers Parquet dans Azure Data Lake
-- Équivalent Impala: tables externes avec partition pruning
-- ============================================================================

-- ============================================================================
-- ÉTAPE 1: Créer la base de données
-- ============================================================================

CREATE DATABASE IF NOT EXISTS FraudDetectionDB;
GO

USE FraudDetectionDB;
GO

-- ============================================================================
-- ÉTAPE 2: Créer les credentials pour accéder au Data Lake
-- ============================================================================

-- Option 1: Managed Identity (recommandé en production)
CREATE DATABASE SCOPED CREDENTIAL SynapseIdentity
WITH IDENTITY = 'Managed Identity';
GO

-- Option 2: Storage Account Key (pour développement)
-- REMPLACER par votre clé!
-- CREATE DATABASE SCOPED CREDENTIAL StorageCredential
-- WITH IDENTITY = 'SHARED ACCESS SIGNATURE',
-- SECRET = 'sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupiytfx&se=2025-12-31...';

-- ============================================================================
-- ÉTAPE 3: Créer la Data Source
-- ============================================================================

-- REMPLACER 'stfrauddetectionxxx' par votre storage account!
CREATE EXTERNAL DATA SOURCE FraudDataLake
WITH (
    LOCATION = 'abfss://processed-data@stfrauddetectionxxx.dfs.core.windows.net',
    CREDENTIAL = SynapseIdentity
);
GO

CREATE EXTERNAL DATA SOURCE AlertsDataLake
WITH (
    LOCATION = 'abfss://fraud-alerts@stfrauddetectionxxx.dfs.core.windows.net',
    CREDENTIAL = SynapseIdentity
);
GO

-- ============================================================================
-- ÉTAPE 4: Créer le File Format pour Parquet
-- ============================================================================

CREATE EXTERNAL FILE FORMAT ParquetFormat
WITH (
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
);
GO

-- ============================================================================
-- ÉTAPE 5: Créer la table externe TRANSACTIONS
-- ============================================================================

CREATE EXTERNAL TABLE dbo.Transactions (
    -- Identifiants
    transactionId           NVARCHAR(100),
    customerId              NVARCHAR(50),
    merchantId              NVARCHAR(50),

    -- Montants
    amount                  FLOAT,
    currency                NVARCHAR(10),
    previousBalance         FLOAT,

    -- Contexte
    channel                 NVARCHAR(20),
    location                NVARCHAR(50),
    country                 NVARCHAR(10),
    cardType                NVARCHAR(20),
    merchantCategory        NVARCHAR(50),

    -- Timestamps
    [timestamp]             BIGINT,
    timestampStr            NVARCHAR(50),
    transactionTime         NVARCHAR(50),
    processedAt             DATETIME2,

    -- Features calculées
    hour                    INT,
    dayOfWeek               INT,
    isSuspiciousHour        BIT,
    isRiskyMerchant         BIT,
    isHighAmount            BIT,
    isInternational         BIT,
    amountToBalanceRatio    FLOAT,
    amountCategory          NVARCHAR(20),

    -- Scoring fraude
    fraudScore              FLOAT,
    riskLevel               NVARCHAR(20),
    isFraudPredicted        BIT,
    alertRequired           BIT,

    -- Labels réels (pour évaluation)
    isFraud                 BIT,
    fraudType               NVARCHAR(50)
)
WITH (
    LOCATION = '/transactions/',
    DATA_SOURCE = FraudDataLake,
    FILE_FORMAT = ParquetFormat
);
GO

-- ============================================================================
-- ÉTAPE 6: Créer la table externe ALERTES
-- ============================================================================

CREATE EXTERNAL TABLE dbo.FraudAlerts (
    transactionId           NVARCHAR(100),
    customerId              NVARCHAR(50),
    merchantId              NVARCHAR(50),
    amount                  FLOAT,
    merchantCategory        NVARCHAR(50),
    location                NVARCHAR(50),
    fraudScore              FLOAT,
    riskLevel               NVARCHAR(20),
    processedAt             DATETIME2,
    transaction_date        DATE
)
WITH (
    LOCATION = '/alerts/',
    DATA_SOURCE = AlertsDataLake,
    FILE_FORMAT = ParquetFormat
);
GO

-- ============================================================================
-- ÉTAPE 7: Créer des vues pour simplifier les requêtes
-- ============================================================================

-- Vue: Métriques temps réel
CREATE VIEW dbo.vw_RealTimeMetrics AS
SELECT
    CAST(transactionTime AS DATE) AS transaction_date,
    COUNT(*) AS total_transactions,
    CAST(SUM(amount) AS DECIMAL(18,2)) AS total_volume,
    CAST(AVG(amount) AS DECIMAL(10,2)) AS avg_amount,
    SUM(CASE WHEN isFraud = 1 THEN 1 ELSE 0 END) AS actual_frauds,
    SUM(CASE WHEN isFraudPredicted = 1 THEN 1 ELSE 0 END) AS predicted_frauds,
    SUM(CASE WHEN alertRequired = 1 THEN 1 ELSE 0 END) AS alerts_generated,
    CAST(AVG(fraudScore) AS DECIMAL(6,4)) AS avg_fraud_score
FROM dbo.Transactions
GROUP BY CAST(transactionTime AS DATE);
GO

-- Vue: Performance du modèle
CREATE VIEW dbo.vw_ModelPerformance AS
SELECT
    riskLevel,
    COUNT(*) AS total_count,
    SUM(CASE WHEN isFraud = 1 THEN 1 ELSE 0 END) AS actual_frauds,
    SUM(CASE WHEN isFraudPredicted = 1 THEN 1 ELSE 0 END) AS predicted_frauds,
    SUM(CASE WHEN isFraudPredicted = 1 AND isFraud = 1 THEN 1 ELSE 0 END) AS true_positives,
    SUM(CASE WHEN isFraudPredicted = 1 AND isFraud = 0 THEN 1 ELSE 0 END) AS false_positives,
    SUM(CASE WHEN isFraudPredicted = 0 AND isFraud = 1 THEN 1 ELSE 0 END) AS false_negatives,
    SUM(CASE WHEN isFraudPredicted = 0 AND isFraud = 0 THEN 1 ELSE 0 END) AS true_negatives,
    CAST(AVG(fraudScore) AS DECIMAL(6,4)) AS avg_fraud_score
FROM dbo.Transactions
GROUP BY riskLevel;
GO

-- Vue: Analyse par catégorie marchand
CREATE VIEW dbo.vw_MerchantAnalysis AS
SELECT
    merchantCategory,
    COUNT(*) AS transaction_count,
    CAST(SUM(amount) AS DECIMAL(18,2)) AS total_volume,
    CAST(AVG(amount) AS DECIMAL(10,2)) AS avg_amount,
    SUM(CASE WHEN isFraud = 1 THEN 1 ELSE 0 END) AS fraud_count,
    CAST(
        100.0 * SUM(CASE WHEN isFraud = 1 THEN 1 ELSE 0 END) / COUNT(*)
        AS DECIMAL(6,2)
    ) AS fraud_rate_percent,
    CAST(AVG(fraudScore) AS DECIMAL(6,4)) AS avg_fraud_score
FROM dbo.Transactions
GROUP BY merchantCategory;
GO

PRINT '✅ Tables externes et vues créées avec succès!';
GO
