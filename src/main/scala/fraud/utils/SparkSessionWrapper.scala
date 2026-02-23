package fraud.utils

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

/**
 * SparkSessionWrapper - Trait pour la gestion de SparkSession
 *
 * Ce trait fournit une session Spark configurée pour le traitement
 * de détection de fraude avec support Kafka et HDFS.
 */
trait SparkSessionWrapper {

  /**
   * Configuration Spark optimisée pour le traitement temps réel
   */
  lazy val sparkConf: SparkConf = new SparkConf()
    .setAppName("RealTimeFraudDetection")
    .set("spark.sql.shuffle.partitions", "200")
    .set("spark.streaming.kafka.maxRatePerPartition", "10000")
    .set("spark.streaming.backpressure.enabled", "true")
    .set("spark.sql.adaptive.enabled", "true")
    .set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryo.registrationRequired", "false")

  /**
   * SparkSession partagée avec configuration Hive et Delta Lake
   */
  lazy val spark: SparkSession = SparkSession.builder()
    .config(sparkConf)
    .enableHiveSupport()
    .getOrCreate()

  /**
   * SparkSession pour mode local (développement/test)
   */
  def localSparkSession(appName: String = "FraudDetectionLocal"): SparkSession = {
    SparkSession.builder()
      .appName(appName)
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.ui.enabled", "true")
      .config("spark.driver.memory", "2g")
      .getOrCreate()
  }

  /**
   * SparkSession pour Spark Streaming avec Kafka
   */
  def streamingSparkSession(
    appName: String,
    kafkaBrokers: String,
    checkpointDir: String
  ): SparkSession = {
    SparkSession.builder()
      .appName(appName)
      .config("spark.streaming.kafka.consumer.cache.enabled", "true")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .config("spark.sql.streaming.checkpointLocation", checkpointDir)
      .getOrCreate()
  }

  /**
   * Fermeture propre de la session
   */
  def closeSession(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }
}

/**
 * Object singleton pour accès global à SparkSession
 */
object SparkSessionManager extends SparkSessionWrapper {

  private var _session: Option[SparkSession] = None

  def getOrCreate(appName: String = "FraudDetection"): SparkSession = {
    _session match {
      case Some(s) if !s.sparkContext.isStopped => s
      case _ =>
        val newSession = SparkSession.builder()
          .appName(appName)
          .config(sparkConf)
          .getOrCreate()
        _session = Some(newSession)
        newSession
    }
  }

  def stop(): Unit = {
    _session.foreach(_.stop())
    _session = None
  }
}
