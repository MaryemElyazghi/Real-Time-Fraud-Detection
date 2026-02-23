package fraud.batch

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window
import fraud.models.Transaction
import fraud.utils.SparkSessionWrapper

/**
 * DataFrameFraudAnalysis - Analyse de fraude avec DataFrame API
 *
 * Cette classe démontre l'utilisation des DataFrames pour le traitement
 * de données structurées avec optimisations Catalyst.
 *
 * Avantages des DataFrames:
 * - Optimisation automatique via Catalyst Optimizer
 * - API déclarative et expressive
 * - Schéma typé et validation
 * - Intégration native avec sources de données
 */
object DataFrameFraudAnalysis extends SparkSessionWrapper {

  // Définition du schéma pour les transactions
  val transactionSchema: StructType = StructType(Seq(
    StructField("transactionId", StringType, nullable = false),
    StructField("customerId", StringType, nullable = false),
    StructField("merchantId", StringType, nullable = false),
    StructField("amount", DoubleType, nullable = false),
    StructField("currency", StringType, nullable = false),
    StructField("transactionType", StringType, nullable = false),
    StructField("channel", StringType, nullable = false),
    StructField("location", StringType, nullable = false),
    StructField("deviceId", StringType, nullable = true),
    StructField("ipAddress", StringType, nullable = true),
    StructField("timestamp", LongType, nullable = false),
    StructField("cardType", StringType, nullable = false),
    StructField("isInternational", BooleanType, nullable = false),
    StructField("merchantCategory", StringType, nullable = false),
    StructField("previousBalance", DoubleType, nullable = false),
    StructField("isFraud", BooleanType, nullable = true)
  ))

  // ==========================================================================
  // CHARGEMENT ET CRÉATION DE DATAFRAMES
  // ==========================================================================

  /**
   * Charger les transactions depuis CSV
   */
  def loadFromCsv(spark: SparkSession, path: String): DataFrame = {
    spark.read
      .option("header", "true")
      .option("inferSchema", "false")
      .schema(transactionSchema)
      .csv(path)
  }

  /**
   * Charger les transactions depuis Parquet (format optimisé)
   */
  def loadFromParquet(spark: SparkSession, path: String): DataFrame = {
    spark.read.parquet(path)
  }

  /**
   * Charger les transactions depuis JSON
   */
  def loadFromJson(spark: SparkSession, path: String): DataFrame = {
    spark.read
      .schema(transactionSchema)
      .json(path)
  }

  /**
   * Convertir RDD en DataFrame avec Dataset API typé
   */
  def toDataset(spark: SparkSession, transactions: Seq[Transaction]): Dataset[Transaction] = {
    import spark.implicits._
    transactions.toDS()
  }

  // ==========================================================================
  // TRANSFORMATIONS DATAFRAME
  // ==========================================================================

  /**
   * Sélection de colonnes - Projection
   */
  def selectBasicInfo(df: DataFrame): DataFrame = {
    df.select(
      col("transactionId"),
      col("customerId"),
      col("amount"),
      col("timestamp"),
      col("isFraud")
    )
  }

  /**
   * Filtrage avec conditions multiples
   */
  def filterHighRiskTransactions(df: DataFrame): DataFrame = {
    df.filter(
      (col("amount") > 5000) ||
      (col("isInternational") === true && col("amount") > 1000) ||
      (col("merchantCategory").isin("GAMBLING", "CRYPTO", "WIRE_TRANSFER"))
    )
  }

  /**
   * Ajout de colonnes calculées
   */
  def addDerivedColumns(df: DataFrame): DataFrame = {
    df
      // Extraire l'heure de la transaction
      .withColumn("hour", (col("timestamp") / 3600000 % 24).cast(IntegerType))
      // Extraire le jour de la semaine
      .withColumn("dayOfWeek",
        dayofweek(from_unixtime(col("timestamp") / 1000)))
      // Flag pour montant élevé
      .withColumn("isHighAmount", col("amount") > 5000)
      // Score de risque initial
      .withColumn("riskScore",
        when(col("amount") > 10000, 0.8)
          .when(col("amount") > 5000, 0.5)
          .when(col("isInternational") && col("amount") > 1000, 0.4)
          .otherwise(0.1)
      )
      // Catégorie de montant
      .withColumn("amountCategory",
        when(col("amount") < 100, "LOW")
          .when(col("amount") < 1000, "MEDIUM")
          .when(col("amount") < 5000, "HIGH")
          .otherwise("VERY_HIGH")
      )
  }

  /**
   * Agrégations par groupe
   */
  def aggregateByCustomer(df: DataFrame): DataFrame = {
    df.groupBy("customerId")
      .agg(
        count("*").as("transactionCount"),
        sum("amount").as("totalAmount"),
        avg("amount").as("avgAmount"),
        max("amount").as("maxAmount"),
        min("amount").as("minAmount"),
        stddev("amount").as("stddevAmount"),
        sum(when(col("isFraud") === true, 1).otherwise(0)).as("fraudCount"),
        countDistinct("merchantId").as("uniqueMerchants"),
        countDistinct("location").as("uniqueLocations")
      )
  }

  /**
   * Agrégations par marchand
   */
  def aggregateByMerchant(df: DataFrame): DataFrame = {
    df.groupBy("merchantId", "merchantCategory")
      .agg(
        count("*").as("transactionCount"),
        sum("amount").as("totalRevenue"),
        avg("amount").as("avgTransactionValue"),
        countDistinct("customerId").as("uniqueCustomers"),
        (sum(when(col("isFraud") === true, 1).otherwise(0)).cast(DoubleType) /
          count("*").cast(DoubleType)).as("fraudRate")
      )
      .orderBy(desc("fraudRate"))
  }

  /**
   * Jointure avec profils clients
   */
  def joinWithCustomerProfiles(
    transactions: DataFrame,
    customerProfiles: DataFrame
  ): DataFrame = {
    transactions.join(
      customerProfiles,
      Seq("customerId"),
      "left_outer"
    )
  }

  // ==========================================================================
  // WINDOW FUNCTIONS - FONCTIONS DE FENÊTRE
  // ==========================================================================

  /**
   * Calculer les statistiques glissantes par client
   */
  def calculateRollingStats(df: DataFrame): DataFrame = {
    // Fenêtre par client ordonnée par timestamp
    val customerWindow = Window
      .partitionBy("customerId")
      .orderBy("timestamp")
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    // Fenêtre pour les 10 dernières transactions
    val recentWindow = Window
      .partitionBy("customerId")
      .orderBy("timestamp")
      .rowsBetween(-9, 0)

    df
      .withColumn("cumulativeAmount", sum("amount").over(customerWindow))
      .withColumn("transactionNumber", row_number().over(customerWindow))
      .withColumn("avgLast10Tx", avg("amount").over(recentWindow))
      .withColumn("maxLast10Tx", max("amount").over(recentWindow))
      .withColumn("txCountLast10", count("*").over(recentWindow))
  }

  /**
   * Détecter les anomalies de vélocité avec fenêtres temporelles
   */
  def detectVelocityAnomalies(df: DataFrame): DataFrame = {
    // Fenêtre de 1 heure par client
    val hourWindow = Window
      .partitionBy("customerId")
      .orderBy(col("timestamp"))
      .rangeBetween(-3600000, 0) // 1 heure en millisecondes

    df
      .withColumn("txCountLastHour", count("*").over(hourWindow))
      .withColumn("amountLastHour", sum("amount").over(hourWindow))
      .withColumn("velocityAnomaly",
        when(col("txCountLastHour") > 10, true)
          .when(col("amountLastHour") > 10000, true)
          .otherwise(false)
      )
  }

  /**
   * Rang et percentiles par catégorie de marchand
   */
  def calculateMerchantRankings(df: DataFrame): DataFrame = {
    val categoryWindow = Window
      .partitionBy("merchantCategory")
      .orderBy(desc("amount"))

    df
      .withColumn("rankInCategory", rank().over(categoryWindow))
      .withColumn("percentileInCategory",
        percent_rank().over(categoryWindow))
      .withColumn("isTopTransaction",
        col("rankInCategory") <= 10)
  }

  // ==========================================================================
  // ANALYSE DE FRAUDE AVANCÉE
  // ==========================================================================

  /**
   * Calculer le score de fraude avec règles multiples
   */
  def calculateFraudScore(df: DataFrame): DataFrame = {
    // Fenêtres pour calculs historiques
    val customerHistory = Window
      .partitionBy("customerId")
      .orderBy("timestamp")
      .rowsBetween(Window.unboundedPreceding, -1)

    df
      // Statistiques historiques du client
      .withColumn("historicalAvg", avg("amount").over(customerHistory))
      .withColumn("historicalStd", stddev("amount").over(customerHistory))
      // Z-score du montant
      .withColumn("amountZScore",
        when(col("historicalStd").isNull || col("historicalStd") === 0, 0)
          .otherwise((col("amount") - col("historicalAvg")) / col("historicalStd"))
      )
      // Score de fraude composite
      .withColumn("fraudScore",
        // Poids pour montant anormal
        when(abs(col("amountZScore")) > 3, 0.4).otherwise(0.0) +
        // Poids pour montant élevé
        when(col("amount") > 5000, 0.2)
          .when(col("amount") > 1000, 0.1)
          .otherwise(0.0) +
        // Poids pour transaction internationale
        when(col("isInternational"), 0.15).otherwise(0.0) +
        // Poids pour catégorie risquée
        when(col("merchantCategory").isin("GAMBLING", "CRYPTO"), 0.2)
          .otherwise(0.0) +
        // Poids pour heure suspecte
        when(hour(from_unixtime(col("timestamp") / 1000)).between(0, 5), 0.15)
          .otherwise(0.0)
      )
      // Classification finale
      .withColumn("riskLevel",
        when(col("fraudScore") >= 0.7, "CRITICAL")
          .when(col("fraudScore") >= 0.5, "HIGH")
          .when(col("fraudScore") >= 0.3, "MEDIUM")
          .otherwise("LOW")
      )
  }

  /**
   * Identifier les patterns de fraude
   */
  def identifyFraudPatterns(df: DataFrame): DataFrame = {
    val customerWindow = Window
      .partitionBy("customerId")
      .orderBy("timestamp")

    df
      .withColumn("prevLocation", lag("location", 1).over(customerWindow))
      .withColumn("prevTimestamp", lag("timestamp", 1).over(customerWindow))
      .withColumn("timeSincePrevTx",
        (col("timestamp") - col("prevTimestamp")) / 1000 / 60) // en minutes
      // Pattern: Voyage impossible (changement de lieu trop rapide)
      .withColumn("impossibleTravel",
        when(
          col("location") =!= col("prevLocation") &&
          col("timeSincePrevTx") < 30 && // Moins de 30 minutes
          col("prevLocation").isNotNull,
          true
        ).otherwise(false)
      )
      // Pattern: Card testing (petits montants répétés)
      .withColumn("smallAmountFlag", col("amount") < 5)
      .withColumn("cardTestingPattern",
        sum(when(col("smallAmountFlag"), 1).otherwise(0))
          .over(Window
            .partitionBy("customerId")
            .orderBy("timestamp")
            .rowsBetween(-4, 0)
          ) >= 3
      )
  }

  // ==========================================================================
  // SAUVEGARDE ET EXPORT
  // ==========================================================================

  /**
   * Sauvegarder en Parquet partitionné
   */
  def saveAsParquet(
    df: DataFrame,
    path: String,
    partitionCols: Seq[String] = Seq("isFraud")
  ): Unit = {
    df.write
      .mode("overwrite")
      .partitionBy(partitionCols: _*)
      .parquet(path)
  }

  /**
   * Sauvegarder pour Impala (format compatible Hive)
   */
  def saveForImpala(
    spark: SparkSession,
    df: DataFrame,
    tableName: String,
    path: String
  ): Unit = {
    // Sauvegarder les données
    df.write
      .mode("overwrite")
      .format("parquet")
      .option("path", path)
      .saveAsTable(tableName)
  }

  /**
   * Point d'entrée principal pour démonstration
   */
  def main(args: Array[String]): Unit = {
    val spark = localSparkSession("DataFrameFraudAnalysis")
    import spark.implicits._

    println("=" * 60)
    println("DataFrame Fraud Analysis - Démonstration")
    println("=" * 60)

    // Créer des données de test
    val transactions = RDDFraudAnalysis.generateSampleTransactions(5000)
    val df = transactions.toDF()

    // Afficher le schéma
    println("\n1. Schéma du DataFrame:")
    df.printSchema()

    // Ajouter colonnes dérivées
    val enrichedDf = addDerivedColumns(df)

    println("\n2. Statistiques par client:")
    aggregateByCustomer(enrichedDf).show(5)

    println("\n3. Transactions avec scores de fraude:")
    val scoredDf = calculateFraudScore(enrichedDf)
    scoredDf.select("transactionId", "amount", "fraudScore", "riskLevel")
      .filter(col("riskLevel").isin("HIGH", "CRITICAL"))
      .show(10)

    println("\n4. Distribution des niveaux de risque:")
    scoredDf.groupBy("riskLevel")
      .count()
      .orderBy(desc("count"))
      .show()

    println("\n5. Détection des patterns de fraude:")
    val patternsDf = identifyFraudPatterns(enrichedDf)
    patternsDf
      .filter(col("impossibleTravel") || col("cardTestingPattern"))
      .select("transactionId", "customerId", "impossibleTravel", "cardTestingPattern")
      .show(10)

    spark.stop()
  }

  /**
   * Génération de transactions pour tests (réutilisation)
   */
  private def generateSampleTransactions(n: Int): Seq[Transaction] = {
    RDDFraudAnalysis.generateSampleTransactions(n)
  }
}
