package fraud.utils

import com.typesafe.config.{Config, ConfigFactory}

/**
 * ConfigManager - Gestion centralisée de la configuration
 *
 * Charge et expose la configuration depuis application.conf
 * avec des valeurs par défaut pour tous les composants.
 */
object ConfigManager {

  private val config: Config = ConfigFactory.load()

  // ==================== KAFKA CONFIGURATION ====================
  object Kafka {
    private val kafkaConfig = config.getConfig("kafka")

    val bootstrapServers: String = kafkaConfig.getString("bootstrap.servers")
    val transactionsTopic: String = kafkaConfig.getString("topics.transactions")
    val fraudAlertsTopic: String = kafkaConfig.getString("topics.fraud-alerts")
    val predictionsTopic: String = kafkaConfig.getString("topics.predictions")

    val consumerGroupId: String = kafkaConfig.getString("consumer.group-id")
    val autoOffsetReset: String = kafkaConfig.getString("consumer.auto-offset-reset")

    val producerAcks: String = kafkaConfig.getString("producer.acks")
    val producerRetries: Int = kafkaConfig.getInt("producer.retries")
  }

  // ==================== SPARK CONFIGURATION ====================
  object Spark {
    private val sparkConfig = config.getConfig("spark")

    val master: String = sparkConfig.getString("master")
    val appName: String = sparkConfig.getString("app-name")
    val checkpointDir: String = sparkConfig.getString("checkpoint-dir")

    val batchInterval: Long = sparkConfig.getLong("streaming.batch-interval")
    val watermarkDelay: String = sparkConfig.getString("streaming.watermark-delay")

    val shufflePartitions: Int = sparkConfig.getInt("sql.shuffle-partitions")
  }

  // ==================== HDFS CONFIGURATION ====================
  object Hdfs {
    private val hdfsConfig = config.getConfig("hdfs")

    val namenode: String = hdfsConfig.getString("namenode")
    val basePath: String = hdfsConfig.getString("base-path")
    val transactionsPath: String = s"$basePath/${hdfsConfig.getString("paths.transactions")}"
    val predictionsPath: String = s"$basePath/${hdfsConfig.getString("paths.predictions")}"
    val modelsPath: String = s"$basePath/${hdfsConfig.getString("paths.models")}"
    val checkpointsPath: String = s"$basePath/${hdfsConfig.getString("paths.checkpoints")}"
  }

  // ==================== IMPALA CONFIGURATION ====================
  object Impala {
    private val impalaConfig = config.getConfig("impala")

    val host: String = impalaConfig.getString("host")
    val port: Int = impalaConfig.getInt("port")
    val database: String = impalaConfig.getString("database")

    val jdbcUrl: String = s"jdbc:impala://$host:$port/$database"
  }

  // ==================== ML MODEL CONFIGURATION ====================
  object MLModel {
    private val mlConfig = config.getConfig("ml")

    val fraudThreshold: Double = mlConfig.getDouble("fraud-threshold")
    val modelPath: String = mlConfig.getString("model-path")
    val trainingDataPath: String = mlConfig.getString("training-data-path")

    val maxIterations: Int = mlConfig.getInt("training.max-iterations")
    val regParam: Double = mlConfig.getDouble("training.reg-param")
    val elasticNetParam: Double = mlConfig.getDouble("training.elastic-net-param")
  }

  // ==================== FRAUD DETECTION RULES ====================
  object FraudRules {
    private val rulesConfig = config.getConfig("fraud-rules")

    val maxAmountThreshold: Double = rulesConfig.getDouble("max-amount-threshold")
    val velocityWindowMinutes: Int = rulesConfig.getInt("velocity-window-minutes")
    val maxTransactionsPerWindow: Int = rulesConfig.getInt("max-transactions-per-window")
    val suspiciousHoursStart: Int = rulesConfig.getInt("suspicious-hours.start")
    val suspiciousHoursEnd: Int = rulesConfig.getInt("suspicious-hours.end")
    val internationalTxMultiplier: Double = rulesConfig.getDouble("international-tx-multiplier")
  }
}
