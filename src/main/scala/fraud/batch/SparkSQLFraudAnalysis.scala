package fraud.batch

import org.apache.spark.sql.{DataFrame, SparkSession}
import fraud.utils.SparkSessionWrapper

/**
 * SparkSQLFraudAnalysis - Analyse de fraude avec Spark SQL
 *
 * Cette classe démontre l'utilisation de Spark SQL pour l'analyse
 * de données avec la syntaxe SQL standard.
 *
 * Avantages de Spark SQL:
 * - Syntaxe SQL familière
 * - Optimisation Catalyst automatique
 * - Intégration avec Hive Metastore
 * - Support des vues temporaires et permanentes
 */
object SparkSQLFraudAnalysis extends SparkSessionWrapper {

  /**
   * Créer une vue temporaire pour les transactions
   */
  def createTransactionsView(spark: SparkSession, df: DataFrame): Unit = {
    df.createOrReplaceTempView("transactions")
  }

  /**
   * Créer une vue temporaire globale (partagée entre sessions)
   */
  def createGlobalView(spark: SparkSession, df: DataFrame): Unit = {
    df.createOrReplaceGlobalTempView("transactions_global")
  }

  // ==========================================================================
  // REQUÊTES SQL DE BASE
  // ==========================================================================

  /**
   * Requête 1: Sélection simple avec filtres
   */
  def basicSelect(spark: SparkSession): DataFrame = {
    spark.sql("""
      SELECT
        transactionId,
        customerId,
        amount,
        merchantCategory,
        isInternational,
        isFraud
      FROM transactions
      WHERE amount > 1000
      ORDER BY amount DESC
      LIMIT 100
    """)
  }

  /**
   * Requête 2: Agrégation par client
   */
  def customerAggregation(spark: SparkSession): DataFrame = {
    spark.sql("""
      SELECT
        customerId,
        COUNT(*) as total_transactions,
        SUM(amount) as total_amount,
        AVG(amount) as avg_amount,
        MAX(amount) as max_amount,
        MIN(amount) as min_amount,
        STDDEV(amount) as stddev_amount,
        SUM(CASE WHEN isFraud = true THEN 1 ELSE 0 END) as fraud_count,
        SUM(CASE WHEN isFraud = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as fraud_rate
      FROM transactions
      GROUP BY customerId
      HAVING COUNT(*) >= 5
      ORDER BY fraud_rate DESC
    """)
  }

  /**
   * Requête 3: Analyse par catégorie de marchand
   */
  def merchantCategoryAnalysis(spark: SparkSession): DataFrame = {
    spark.sql("""
      SELECT
        merchantCategory,
        COUNT(*) as transaction_count,
        SUM(amount) as total_volume,
        AVG(amount) as avg_transaction,
        COUNT(DISTINCT customerId) as unique_customers,
        SUM(CASE WHEN isFraud = true THEN 1 ELSE 0 END) as fraud_count,
        ROUND(
          SUM(CASE WHEN isFraud = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
          2
        ) as fraud_rate_percent
      FROM transactions
      GROUP BY merchantCategory
      ORDER BY fraud_rate_percent DESC
    """)
  }

  /**
   * Requête 4: Transactions par heure de la journée
   */
  def hourlyDistribution(spark: SparkSession): DataFrame = {
    spark.sql("""
      SELECT
        CAST((timestamp / 3600000) % 24 AS INT) as hour_of_day,
        COUNT(*) as transaction_count,
        SUM(amount) as total_amount,
        AVG(amount) as avg_amount,
        SUM(CASE WHEN isFraud = true THEN 1 ELSE 0 END) as fraud_count,
        ROUND(
          SUM(CASE WHEN isFraud = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
          2
        ) as fraud_rate
      FROM transactions
      GROUP BY CAST((timestamp / 3600000) % 24 AS INT)
      ORDER BY hour_of_day
    """)
  }

  // ==========================================================================
  // REQUÊTES SQL AVANCÉES - DÉTECTION DE FRAUDE
  // ==========================================================================

  /**
   * Requête 5: Transactions à haut risque (règles multiples)
   */
  def highRiskTransactions(spark: SparkSession): DataFrame = {
    spark.sql("""
      SELECT
        transactionId,
        customerId,
        amount,
        merchantCategory,
        location,
        isInternational,
        CAST((timestamp / 3600000) % 24 AS INT) as hour,
        CASE
          WHEN amount > 10000 THEN 'VERY_HIGH_AMOUNT'
          WHEN amount > 5000 THEN 'HIGH_AMOUNT'
          WHEN isInternational = true AND amount > 1000 THEN 'HIGH_INTERNATIONAL'
          WHEN merchantCategory IN ('GAMBLING', 'CRYPTO', 'WIRE_TRANSFER') THEN 'RISKY_MERCHANT'
          WHEN CAST((timestamp / 3600000) % 24 AS INT) BETWEEN 0 AND 5 THEN 'SUSPICIOUS_HOUR'
          ELSE 'NORMAL'
        END as risk_reason,
        CASE
          WHEN amount > 10000 OR
               (isInternational = true AND amount > 5000) OR
               merchantCategory IN ('GAMBLING', 'CRYPTO')
          THEN 'CRITICAL'
          WHEN amount > 5000 OR
               (isInternational = true AND amount > 1000) OR
               CAST((timestamp / 3600000) % 24 AS INT) BETWEEN 0 AND 5
          THEN 'HIGH'
          WHEN amount > 1000
          THEN 'MEDIUM'
          ELSE 'LOW'
        END as risk_level
      FROM transactions
      WHERE amount > 1000
         OR isInternational = true
         OR merchantCategory IN ('GAMBLING', 'CRYPTO', 'WIRE_TRANSFER')
         OR CAST((timestamp / 3600000) % 24 AS INT) BETWEEN 0 AND 5
      ORDER BY
        CASE
          WHEN risk_level = 'CRITICAL' THEN 1
          WHEN risk_level = 'HIGH' THEN 2
          WHEN risk_level = 'MEDIUM' THEN 3
          ELSE 4
        END,
        amount DESC
    """)
  }

  /**
   * Requête 6: Sous-requête - Clients avec comportement anormal
   */
  def abnormalCustomerBehavior(spark: SparkSession): DataFrame = {
    spark.sql("""
      WITH customer_stats AS (
        SELECT
          customerId,
          AVG(amount) as avg_amount,
          STDDEV(amount) as stddev_amount,
          COUNT(*) as tx_count
        FROM transactions
        GROUP BY customerId
        HAVING COUNT(*) >= 3
      )
      SELECT
        t.transactionId,
        t.customerId,
        t.amount,
        cs.avg_amount,
        cs.stddev_amount,
        (t.amount - cs.avg_amount) / NULLIF(cs.stddev_amount, 0) as z_score,
        CASE
          WHEN ABS((t.amount - cs.avg_amount) / NULLIF(cs.stddev_amount, 0)) > 3 THEN 'EXTREME_OUTLIER'
          WHEN ABS((t.amount - cs.avg_amount) / NULLIF(cs.stddev_amount, 0)) > 2 THEN 'OUTLIER'
          ELSE 'NORMAL'
        END as anomaly_status
      FROM transactions t
      JOIN customer_stats cs ON t.customerId = cs.customerId
      WHERE ABS((t.amount - cs.avg_amount) / NULLIF(cs.stddev_amount, 0)) > 2
      ORDER BY ABS((t.amount - cs.avg_amount) / NULLIF(cs.stddev_amount, 0)) DESC
    """)
  }

  /**
   * Requête 7: Window Functions - Analyse de vélocité
   */
  def velocityAnalysis(spark: SparkSession): DataFrame = {
    spark.sql("""
      SELECT
        transactionId,
        customerId,
        amount,
        timestamp,
        LAG(timestamp, 1) OVER (PARTITION BY customerId ORDER BY timestamp) as prev_timestamp,
        (timestamp - LAG(timestamp, 1) OVER (PARTITION BY customerId ORDER BY timestamp)) / 1000 / 60 as minutes_since_prev,
        LAG(location, 1) OVER (PARTITION BY customerId ORDER BY timestamp) as prev_location,
        location,
        CASE
          WHEN location != LAG(location, 1) OVER (PARTITION BY customerId ORDER BY timestamp)
           AND (timestamp - LAG(timestamp, 1) OVER (PARTITION BY customerId ORDER BY timestamp)) / 1000 / 60 < 30
          THEN true
          ELSE false
        END as impossible_travel,
        COUNT(*) OVER (
          PARTITION BY customerId
          ORDER BY timestamp
          RANGE BETWEEN 3600000 PRECEDING AND CURRENT ROW
        ) as tx_count_last_hour,
        SUM(amount) OVER (
          PARTITION BY customerId
          ORDER BY timestamp
          RANGE BETWEEN 3600000 PRECEDING AND CURRENT ROW
        ) as amount_last_hour,
        ROW_NUMBER() OVER (PARTITION BY customerId ORDER BY timestamp) as tx_sequence
      FROM transactions
      ORDER BY customerId, timestamp
    """)
  }

  /**
   * Requête 8: Analyse temporelle avec CUBE
   */
  def temporalAnalysisCube(spark: SparkSession): DataFrame = {
    spark.sql("""
      SELECT
        COALESCE(CAST(merchantCategory AS STRING), 'ALL') as merchant_category,
        COALESCE(CAST(channel AS STRING), 'ALL') as channel,
        COALESCE(CAST(cardType AS STRING), 'ALL') as card_type,
        COUNT(*) as transaction_count,
        SUM(amount) as total_amount,
        AVG(amount) as avg_amount,
        SUM(CASE WHEN isFraud = true THEN 1 ELSE 0 END) as fraud_count
      FROM transactions
      GROUP BY CUBE (merchantCategory, channel, cardType)
      ORDER BY transaction_count DESC
    """)
  }

  /**
   * Requête 9: Percentiles et quartiles par catégorie
   */
  def percentileAnalysis(spark: SparkSession): DataFrame = {
    spark.sql("""
      SELECT
        merchantCategory,
        PERCENTILE_APPROX(amount, 0.25) as q1,
        PERCENTILE_APPROX(amount, 0.50) as median,
        PERCENTILE_APPROX(amount, 0.75) as q3,
        PERCENTILE_APPROX(amount, 0.90) as p90,
        PERCENTILE_APPROX(amount, 0.95) as p95,
        PERCENTILE_APPROX(amount, 0.99) as p99,
        MAX(amount) as max_amount
      FROM transactions
      GROUP BY merchantCategory
      ORDER BY median DESC
    """)
  }

  /**
   * Requête 10: Détection de patterns avec auto-jointure
   */
  def patternDetection(spark: SparkSession): DataFrame = {
    spark.sql("""
      WITH numbered_transactions AS (
        SELECT
          *,
          ROW_NUMBER() OVER (PARTITION BY customerId ORDER BY timestamp) as tx_num
        FROM transactions
      )
      SELECT
        t1.customerId,
        t1.transactionId as first_tx,
        t2.transactionId as second_tx,
        t1.amount as first_amount,
        t2.amount as second_amount,
        t1.location as first_location,
        t2.location as second_location,
        (t2.timestamp - t1.timestamp) / 1000 / 60 as minutes_between,
        CASE
          WHEN t1.location != t2.location
           AND (t2.timestamp - t1.timestamp) / 1000 / 60 < 30
          THEN 'IMPOSSIBLE_TRAVEL'
          WHEN t1.amount < 5 AND t2.amount < 5 AND t2.amount > t1.amount
          THEN 'CARD_TESTING'
          WHEN t2.amount > t1.amount * 10
          THEN 'SUDDEN_INCREASE'
          ELSE 'NORMAL'
        END as pattern_type
      FROM numbered_transactions t1
      JOIN numbered_transactions t2
        ON t1.customerId = t2.customerId
        AND t2.tx_num = t1.tx_num + 1
      WHERE (
        (t1.location != t2.location AND (t2.timestamp - t1.timestamp) / 1000 / 60 < 30) OR
        (t1.amount < 5 AND t2.amount < 5) OR
        (t2.amount > t1.amount * 10)
      )
      ORDER BY t1.customerId, t1.timestamp
    """)
  }

  // ==========================================================================
  // REQUÊTES POUR TABLEAUX DE BORD
  // ==========================================================================

  /**
   * Statistiques globales pour dashboard
   */
  def dashboardMetrics(spark: SparkSession): DataFrame = {
    spark.sql("""
      SELECT
        COUNT(*) as total_transactions,
        SUM(amount) as total_volume,
        AVG(amount) as avg_transaction_amount,
        COUNT(DISTINCT customerId) as unique_customers,
        COUNT(DISTINCT merchantId) as unique_merchants,
        SUM(CASE WHEN isFraud = true THEN 1 ELSE 0 END) as total_frauds,
        ROUND(SUM(CASE WHEN isFraud = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 4) as fraud_rate_percent,
        SUM(CASE WHEN isFraud = true THEN amount ELSE 0 END) as fraud_volume
      FROM transactions
    """)
  }

  /**
   * Tendances par période
   */
  def trendAnalysis(spark: SparkSession): DataFrame = {
    spark.sql("""
      SELECT
        DATE(FROM_UNIXTIME(timestamp / 1000)) as date,
        HOUR(FROM_UNIXTIME(timestamp / 1000)) as hour,
        COUNT(*) as transaction_count,
        SUM(amount) as total_amount,
        SUM(CASE WHEN isFraud = true THEN 1 ELSE 0 END) as fraud_count,
        AVG(CASE WHEN isFraud = true THEN 1 ELSE 0 END) * 100 as fraud_rate
      FROM transactions
      GROUP BY
        DATE(FROM_UNIXTIME(timestamp / 1000)),
        HOUR(FROM_UNIXTIME(timestamp / 1000))
      ORDER BY date, hour
    """)
  }

  /**
   * Top marchands par volume de fraude
   */
  def topFraudMerchants(spark: SparkSession): DataFrame = {
    spark.sql("""
      SELECT
        merchantId,
        merchantCategory,
        COUNT(*) as total_transactions,
        SUM(CASE WHEN isFraud = true THEN 1 ELSE 0 END) as fraud_count,
        SUM(CASE WHEN isFraud = true THEN amount ELSE 0 END) as fraud_volume,
        ROUND(SUM(CASE WHEN isFraud = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as fraud_rate
      FROM transactions
      GROUP BY merchantId, merchantCategory
      HAVING SUM(CASE WHEN isFraud = true THEN 1 ELSE 0 END) > 0
      ORDER BY fraud_rate DESC, fraud_volume DESC
      LIMIT 20
    """)
  }

  // ==========================================================================
  // TABLES EXTERNES POUR IMPALA
  // ==========================================================================

  /**
   * Créer une table externe Hive/Impala
   */
  def createExternalTable(spark: SparkSession, tableName: String, path: String): Unit = {
    spark.sql(s"""
      CREATE EXTERNAL TABLE IF NOT EXISTS $tableName (
        transactionId STRING,
        customerId STRING,
        merchantId STRING,
        amount DOUBLE,
        currency STRING,
        transactionType STRING,
        channel STRING,
        location STRING,
        deviceId STRING,
        ipAddress STRING,
        timestamp BIGINT,
        cardType STRING,
        isInternational BOOLEAN,
        merchantCategory STRING,
        previousBalance DOUBLE,
        isFraud BOOLEAN
      )
      STORED AS PARQUET
      LOCATION '$path'
    """)
  }

  /**
   * Créer une table partitionnée pour Impala
   */
  def createPartitionedTable(spark: SparkSession, tableName: String, path: String): Unit = {
    spark.sql(s"""
      CREATE EXTERNAL TABLE IF NOT EXISTS ${tableName}_partitioned (
        transactionId STRING,
        customerId STRING,
        merchantId STRING,
        amount DOUBLE,
        currency STRING,
        transactionType STRING,
        channel STRING,
        location STRING,
        deviceId STRING,
        ipAddress STRING,
        timestamp BIGINT,
        cardType STRING,
        isInternational BOOLEAN,
        merchantCategory STRING,
        previousBalance DOUBLE
      )
      PARTITIONED BY (isFraud BOOLEAN, transaction_date DATE)
      STORED AS PARQUET
      LOCATION '$path'
    """)
  }

  /**
   * Point d'entrée principal pour démonstration
   */
  def main(args: Array[String]): Unit = {
    val spark = localSparkSession("SparkSQLFraudAnalysis")

    println("=" * 60)
    println("Spark SQL Fraud Analysis - Démonstration")
    println("=" * 60)

    // Créer des données de test
    import spark.implicits._
    val transactions = RDDFraudAnalysis.generateSampleTransactions(5000)
    val df = transactions.toDF()

    // Créer la vue temporaire
    createTransactionsView(spark, df)

    println("\n1. Statistiques dashboard:")
    dashboardMetrics(spark).show()

    println("\n2. Agrégation par catégorie de marchand:")
    merchantCategoryAnalysis(spark).show()

    println("\n3. Distribution par heure:")
    hourlyDistribution(spark).show(24)

    println("\n4. Transactions à haut risque:")
    highRiskTransactions(spark).show(10)

    println("\n5. Comportements anormaux des clients:")
    abnormalCustomerBehavior(spark).show(10)

    println("\n6. Analyse de vélocité:")
    velocityAnalysis(spark).show(10)

    println("\n7. Détection de patterns:")
    patternDetection(spark).show(10)

    spark.stop()
  }
}
