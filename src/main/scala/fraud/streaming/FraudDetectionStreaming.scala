package fraud.streaming

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.types._
import fraud.utils.{ConfigManager, SparkSessionWrapper}
import fraud.models.{Transaction, FraudPrediction}

/**
 * FraudDetectionStreaming - Pipeline de détection de fraude en temps réel
 *
 * Ce module implémente un pipeline complet de streaming structuré:
 * Kafka → Spark Structured Streaming → HDFS/Kafka
 *
 * Fonctionnalités:
 * - Ingestion temps réel depuis Kafka
 * - Transformation et enrichissement des données
 * - Détection de fraude basée sur règles
 * - Écriture vers HDFS et Kafka (alertes)
 * - Gestion des watermarks et fenêtres temporelles
 */
object FraudDetectionStreaming extends SparkSessionWrapper {

  // Schéma JSON pour les transactions Kafka
  val transactionSchema: StructType = StructType(Seq(
    StructField("transactionId", StringType),
    StructField("customerId", StringType),
    StructField("merchantId", StringType),
    StructField("amount", DoubleType),
    StructField("currency", StringType),
    StructField("transactionType", StringType),
    StructField("channel", StringType),
    StructField("location", StringType),
    StructField("deviceId", StringType),
    StructField("ipAddress", StringType),
    StructField("timestamp", LongType),
    StructField("cardType", StringType),
    StructField("isInternational", BooleanType),
    StructField("merchantCategory", StringType),
    StructField("previousBalance", DoubleType)
  ))

  /**
   * Démarrer le pipeline de streaming principal
   */
  def startStreamingPipeline(spark: SparkSession): StreamingQuery = {
    import spark.implicits._

    // 1. Lire depuis Kafka
    val kafkaStream = readFromKafka(spark)

    // 2. Parser le JSON et extraire les transactions
    val transactions = parseTransactions(kafkaStream)

    // 3. Enrichir avec des features de détection
    val enrichedTransactions = enrichTransactions(transactions)

    // 4. Appliquer les règles de détection de fraude
    val scoredTransactions = applyFraudRules(enrichedTransactions)

    // 5. Filtrer les alertes (transactions suspectes)
    val alerts = scoredTransactions.filter(col("fraudScore") >= 0.5)

    // 6. Écrire vers HDFS (toutes les transactions)
    writeToHDFS(scoredTransactions, ConfigManager.Hdfs.transactionsPath)

    // 7. Écrire les alertes vers Kafka
    writeAlertsToKafka(alerts)
  }

  /**
   * Lire le stream depuis Kafka
   */
  def readFromKafka(spark: SparkSession): DataFrame = {
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", ConfigManager.Kafka.bootstrapServers)
      .option("subscribe", ConfigManager.Kafka.transactionsTopic)
      .option("startingOffsets", "latest")
      .option("maxOffsetsPerTrigger", 10000)
      .option("failOnDataLoss", "false")
      .load()
  }

  /**
   * Parser les messages Kafka en transactions structurées
   */
  def parseTransactions(kafkaStream: DataFrame): DataFrame = {
    kafkaStream
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp as kafka_timestamp")
      .withColumn("transaction", from_json(col("value"), transactionSchema))
      .select(
        col("kafka_timestamp"),
        col("transaction.*")
      )
      // Ajouter le watermark pour la gestion des données tardives
      .withWatermark("kafka_timestamp", ConfigManager.Spark.watermarkDelay)
  }

  /**
   * Enrichir les transactions avec des features calculées
   */
  def enrichTransactions(transactions: DataFrame): DataFrame = {
    transactions
      // Extraire l'heure et le jour
      .withColumn("hour", hour(col("kafka_timestamp")))
      .withColumn("dayOfWeek", dayofweek(col("kafka_timestamp")))
      .withColumn("isWeekend",
        col("dayOfWeek").isin(1, 7))
      // Flag pour heure suspecte (minuit à 5h)
      .withColumn("isSuspiciousHour",
        col("hour").between(
          ConfigManager.FraudRules.suspiciousHoursStart,
          ConfigManager.FraudRules.suspiciousHoursEnd
        ))
      // Catégoriser le montant
      .withColumn("amountCategory",
        when(col("amount") < 100, "LOW")
          .when(col("amount") < 1000, "MEDIUM")
          .when(col("amount") < 5000, "HIGH")
          .otherwise("VERY_HIGH"))
      // Flag pour catégories à risque
      .withColumn("isRiskyMerchant",
        col("merchantCategory").isin("GAMBLING", "CRYPTO", "WIRE_TRANSFER"))
  }

  /**
   * Appliquer les règles de détection de fraude
   */
  def applyFraudRules(transactions: DataFrame): DataFrame = {
    transactions
      // Calculer le score de fraude basé sur les règles
      .withColumn("fraudScore",
        // Montant très élevé
        when(col("amount") > 10000, lit(0.4))
          .when(col("amount") > ConfigManager.FraudRules.maxAmountThreshold, lit(0.3))
          .otherwise(lit(0.0)) +
        // Transaction internationale + montant élevé
        when(col("isInternational") && col("amount") > 1000,
          lit(0.2) * ConfigManager.FraudRules.internationalTxMultiplier)
          .otherwise(lit(0.0)) +
        // Heure suspecte
        when(col("isSuspiciousHour"), lit(0.15)).otherwise(lit(0.0)) +
        // Marchand à risque
        when(col("isRiskyMerchant"), lit(0.25)).otherwise(lit(0.0)) +
        // Weekend + montant élevé
        when(col("isWeekend") && col("amount") > 2000, lit(0.1))
          .otherwise(lit(0.0))
      )
      // Classification du risque
      .withColumn("riskLevel",
        when(col("fraudScore") >= 0.7, "CRITICAL")
          .when(col("fraudScore") >= 0.5, "HIGH")
          .when(col("fraudScore") >= 0.3, "MEDIUM")
          .otherwise("LOW"))
      // Décision de fraude
      .withColumn("isFraudPredicted",
        col("fraudScore") >= ConfigManager.MLModel.fraudThreshold)
      // Raisons de l'alerte
      .withColumn("alertReasons",
        array_remove(
          array(
            when(col("amount") > ConfigManager.FraudRules.maxAmountThreshold,
              lit("HIGH_AMOUNT")).otherwise(lit(null)),
            when(col("isInternational") && col("amount") > 1000,
              lit("HIGH_INTERNATIONAL_TX")).otherwise(lit(null)),
            when(col("isSuspiciousHour"),
              lit("SUSPICIOUS_HOUR")).otherwise(lit(null)),
            when(col("isRiskyMerchant"),
              lit("RISKY_MERCHANT")).otherwise(lit(null))
          ),
          lit(null)
        )
      )
  }

  /**
   * Écrire vers HDFS en format Parquet partitionné
   */
  def writeToHDFS(transactions: DataFrame, outputPath: String): StreamingQuery = {
    transactions.writeStream
      .format("parquet")
      .option("path", outputPath)
      .option("checkpointLocation", s"${ConfigManager.Hdfs.checkpointsPath}/transactions")
      .partitionBy("riskLevel")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .outputMode("append")
      .start()
  }

  /**
   * Écrire les alertes vers Kafka
   */
  def writeAlertsToKafka(alerts: DataFrame): StreamingQuery = {
    // Préparer le message JSON pour Kafka
    val alertsKafka = alerts
      .select(
        col("transactionId").as("key"),
        to_json(struct(
          col("transactionId"),
          col("customerId"),
          col("amount"),
          col("fraudScore"),
          col("riskLevel"),
          col("alertReasons"),
          col("kafka_timestamp").as("processedAt")
        )).as("value")
      )

    alertsKafka.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", ConfigManager.Kafka.bootstrapServers)
      .option("topic", ConfigManager.Kafka.fraudAlertsTopic)
      .option("checkpointLocation", s"${ConfigManager.Hdfs.checkpointsPath}/alerts")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .outputMode("append")
      .start()
  }

  // ==========================================================================
  // ANALYSES AVEC FENÊTRES TEMPORELLES
  // ==========================================================================

  /**
   * Agrégation par fenêtre temporelle pour détection de vélocité
   */
  def velocityDetection(spark: SparkSession): StreamingQuery = {
    val kafkaStream = readFromKafka(spark)
    val transactions = parseTransactions(kafkaStream)

    // Fenêtre de 5 minutes avec slide de 1 minute
    val velocityStats = transactions
      .groupBy(
        col("customerId"),
        window(col("kafka_timestamp"), "5 minutes", "1 minute")
      )
      .agg(
        count("*").as("txCount"),
        sum("amount").as("totalAmount"),
        avg("amount").as("avgAmount"),
        max("amount").as("maxAmount"),
        collect_set("location").as("locations")
      )
      // Détecter vélocité anormale
      .withColumn("isVelocityAnomaly",
        col("txCount") > ConfigManager.FraudRules.maxTransactionsPerWindow)
      .withColumn("isAmountAnomaly",
        col("totalAmount") > 10000)
      // Multi-localisation suspecte
      .withColumn("isMultiLocationAnomaly",
        size(col("locations")) > 3)

    // Filtrer les anomalies
    val anomalies = velocityStats.filter(
      col("isVelocityAnomaly") ||
      col("isAmountAnomaly") ||
      col("isMultiLocationAnomaly")
    )

    anomalies.writeStream
      .format("console")
      .option("truncate", "false")
      .outputMode("update")
      .trigger(Trigger.ProcessingTime("30 seconds"))
      .start()
  }

  /**
   * Analyse de pattern avec fenêtre de session
   */
  def sessionPatternAnalysis(spark: SparkSession): StreamingQuery = {
    val kafkaStream = readFromKafka(spark)
    val transactions = parseTransactions(kafkaStream)

    // Fenêtre de session avec gap de 10 minutes
    val sessionStats = transactions
      .groupBy(
        col("customerId"),
        session_window(col("kafka_timestamp"), "10 minutes")
      )
      .agg(
        count("*").as("sessionTxCount"),
        sum("amount").as("sessionTotalAmount"),
        first("location").as("firstLocation"),
        last("location").as("lastLocation"),
        collect_list("merchantCategory").as("merchantCategories")
      )
      // Patterns suspects
      .withColumn("hasLocationChange",
        col("firstLocation") =!= col("lastLocation"))
      .withColumn("sessionDurationMinutes",
        (unix_timestamp(col("session_window.end")) -
          unix_timestamp(col("session_window.start"))) / 60)

    sessionStats.writeStream
      .format("memory")
      .queryName("session_patterns")
      .outputMode("update")
      .trigger(Trigger.ProcessingTime("1 minute"))
      .start()
  }

  // ==========================================================================
  // STREAMING AVEC JOINTURE ÉTAT
  // ==========================================================================

  /**
   * Jointure avec profils clients en streaming
   */
  def streamWithCustomerProfiles(
    spark: SparkSession,
    customerProfilesPath: String
  ): StreamingQuery = {
    import spark.implicits._

    // Charger les profils clients (statique)
    val customerProfiles = spark.read
      .parquet(customerProfilesPath)
      .select(
        col("customerId"),
        col("avgAmount").as("profileAvgAmount"),
        col("maxAmount").as("profileMaxAmount"),
        col("fraudCount").as("historyFraudCount")
      )

    // Stream des transactions
    val kafkaStream = readFromKafka(spark)
    val transactions = parseTransactions(kafkaStream)

    // Stream-static join
    val enrichedWithProfile = transactions
      .join(customerProfiles, Seq("customerId"), "left_outer")
      .withColumn("amountDeviation",
        when(col("profileAvgAmount").isNotNull,
          (col("amount") - col("profileAvgAmount")) / col("profileAvgAmount"))
          .otherwise(lit(0.0)))
      .withColumn("exceedsHistoricalMax",
        col("amount") > col("profileMaxAmount"))
      .withColumn("hasHistoricalFraud",
        col("historyFraudCount") > 0)

    // Score ajusté avec le profil
    val finalScored = enrichedWithProfile
      .withColumn("adjustedFraudScore",
        when(col("exceedsHistoricalMax"), col("fraudScore") + 0.2)
          .when(col("amountDeviation") > 2, col("fraudScore") + 0.15)
          .when(col("hasHistoricalFraud"), col("fraudScore") + 0.1)
          .otherwise(col("fraudScore"))
      )

    finalScored.writeStream
      .format("parquet")
      .option("path", s"${ConfigManager.Hdfs.basePath}/enriched_transactions")
      .option("checkpointLocation", s"${ConfigManager.Hdfs.checkpointsPath}/enriched")
      .outputMode("append")
      .start()
  }

  // ==========================================================================
  // MÉTRIQUES ET MONITORING
  // ==========================================================================

  /**
   * Calcul des métriques en temps réel pour dashboard
   */
  def realTimeMetrics(spark: SparkSession): StreamingQuery = {
    val kafkaStream = readFromKafka(spark)
    val transactions = parseTransactions(kafkaStream)
    val scored = applyFraudRules(enrichTransactions(transactions))

    // Métriques agrégées par fenêtre de 1 minute
    val metrics = scored
      .groupBy(window(col("kafka_timestamp"), "1 minute"))
      .agg(
        count("*").as("totalTransactions"),
        sum("amount").as("totalVolume"),
        avg("amount").as("avgAmount"),
        sum(when(col("isFraudPredicted"), 1).otherwise(0)).as("fraudCount"),
        sum(when(col("riskLevel") === "CRITICAL", 1).otherwise(0)).as("criticalCount"),
        sum(when(col("riskLevel") === "HIGH", 1).otherwise(0)).as("highRiskCount"),
        avg("fraudScore").as("avgFraudScore")
      )
      .withColumn("fraudRate",
        col("fraudCount").cast("double") / col("totalTransactions"))

    metrics.writeStream
      .format("console")
      .option("truncate", "false")
      .outputMode("update")
      .trigger(Trigger.ProcessingTime("1 minute"))
      .start()
  }

  /**
   * Point d'entrée principal
   */
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("FraudDetectionStreaming")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()

    println("=" * 60)
    println("Real-Time Fraud Detection Streaming Pipeline")
    println("=" * 60)
    println(s"Kafka Brokers: ${ConfigManager.Kafka.bootstrapServers}")
    println(s"Input Topic: ${ConfigManager.Kafka.transactionsTopic}")
    println(s"Alerts Topic: ${ConfigManager.Kafka.fraudAlertsTopic}")
    println("=" * 60)

    // Démarrer le pipeline principal
    val mainQuery = startStreamingPipeline(spark)

    // Démarrer les métriques temps réel
    val metricsQuery = realTimeMetrics(spark)

    // Attendre la terminaison
    spark.streams.awaitAnyTermination()
  }
}
