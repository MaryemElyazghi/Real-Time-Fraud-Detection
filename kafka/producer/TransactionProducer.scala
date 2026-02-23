package fraud.kafka

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, Callback, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer
import fraud.models.Transaction
import fraud.utils.ConfigManager
import scala.util.Random
import java.util.concurrent.{Executors, TimeUnit}

/**
 * TransactionProducer - Producteur Kafka de transactions
 *
 * Ce producteur simule un flux de transactions financières en temps réel.
 * Il génère des transactions normales et frauduleuses selon un ratio configurable.
 *
 * Caractéristiques:
 * - Production haute performance (10K+ messages/seconde)
 * - Transactions réalistes avec patterns de fraude
 * - Callbacks asynchrones pour confirmation
 * - Métriques de production en temps réel
 */
object TransactionProducer {

  // Données de simulation
  private val locations = Seq(
    "Paris", "London", "New York", "Tokyo", "Singapore", "Dubai",
    "Hong Kong", "Sydney", "Berlin", "Madrid", "Amsterdam", "Milan"
  )

  private val merchantCategories = Seq(
    "RETAIL", "GROCERY", "RESTAURANT", "ONLINE", "TRAVEL",
    "ENTERTAINMENT", "UTILITIES", "HEALTHCARE", "GAMBLING", "CRYPTO"
  )

  private val channels = Seq("ONLINE", "POS", "ATM", "MOBILE")
  private val currencies = Seq("EUR", "USD", "GBP", "CHF")
  private val cardTypes = Seq("CREDIT", "DEBIT")

  private val random = new Random(System.currentTimeMillis())

  /**
   * Créer une instance de KafkaProducer
   */
  def createProducer(): KafkaProducer[String, String] = {
    val props = new Properties()

    // Configuration du broker
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ConfigManager.Kafka.bootstrapServers)

    // Sérialiseurs
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

    // Configuration de performance
    props.put(ProducerConfig.ACKS_CONFIG, ConfigManager.Kafka.producerAcks)
    props.put(ProducerConfig.RETRIES_CONFIG, ConfigManager.Kafka.producerRetries.toString)
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384")
    props.put(ProducerConfig.LINGER_MS_CONFIG, "5")
    props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432")

    // Compression pour réduire la latence réseau
    props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy")

    // Idempotence pour exactly-once semantics
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")

    new KafkaProducer[String, String](props)
  }

  /**
   * Générer une transaction normale
   */
  def generateNormalTransaction(customerId: String): Transaction = {
    val amount = random.nextGaussian() * 150 + 200 // Moyenne ~200, écart-type ~150
    val normalizedAmount = math.max(5, math.abs(amount)) // Minimum 5

    Transaction(
      transactionId = s"TX${System.currentTimeMillis()}-${random.nextInt(10000)}",
      customerId = customerId,
      merchantId = s"MERCH${random.nextInt(1000)}",
      amount = math.round(normalizedAmount * 100) / 100.0,
      currency = currencies(random.nextInt(currencies.length)),
      transactionType = "PURCHASE",
      channel = channels(random.nextInt(channels.length)),
      location = locations(random.nextInt(locations.length)),
      deviceId = Some(s"DEV${random.nextInt(5000)}"),
      ipAddress = Some(s"192.168.${random.nextInt(256)}.${random.nextInt(256)}"),
      timestamp = System.currentTimeMillis(),
      cardType = cardTypes(random.nextInt(cardTypes.length)),
      isInternational = random.nextDouble() < 0.1,
      merchantCategory = merchantCategories.take(8)(random.nextInt(8)), // Exclure GAMBLING, CRYPTO
      previousBalance = random.nextDouble() * 10000 + 1000,
      isFraud = Some(false)
    )
  }

  /**
   * Générer une transaction frauduleuse
   * Applique des patterns de fraude réalistes
   */
  def generateFraudulentTransaction(customerId: String): Transaction = {
    val fraudType = random.nextInt(5)

    val (amount, category, isIntl, hour) = fraudType match {
      // Type 1: Montant très élevé
      case 0 =>
        (random.nextDouble() * 15000 + 5000, "ONLINE", false, random.nextInt(24))

      // Type 2: Transaction internationale suspecte
      case 1 =>
        (random.nextDouble() * 3000 + 1000, "TRAVEL", true, random.nextInt(24))

      // Type 3: Gambling/Crypto
      case 2 =>
        val cat = if (random.nextBoolean()) "GAMBLING" else "CRYPTO"
        (random.nextDouble() * 5000 + 500, cat, random.nextBoolean(), random.nextInt(24))

      // Type 4: Transaction de nuit
      case 3 =>
        (random.nextDouble() * 2000 + 500, "ATM", false, random.nextInt(5)) // 0-5h

      // Type 5: Card testing (petits montants rapides)
      case 4 =>
        (random.nextDouble() * 10 + 0.5, "ONLINE", false, random.nextInt(24))

      case _ =>
        (random.nextDouble() * 5000 + 1000, "ONLINE", false, random.nextInt(24))
    }

    // Ajuster le timestamp pour l'heure de fraude
    val baseTimestamp = System.currentTimeMillis()
    val adjustedTimestamp = if (fraudType == 3) {
      // Mettre l'heure entre 0 et 5
      val currentHour = (baseTimestamp / 3600000) % 24
      val targetHour = random.nextInt(5)
      baseTimestamp - (currentHour - targetHour) * 3600000
    } else {
      baseTimestamp
    }

    Transaction(
      transactionId = s"TX${System.currentTimeMillis()}-${random.nextInt(10000)}",
      customerId = customerId,
      merchantId = s"MERCH${random.nextInt(1000)}",
      amount = math.round(amount * 100) / 100.0,
      currency = if (isIntl) currencies(random.nextInt(currencies.length)) else "EUR",
      transactionType = if (category == "ATM") "WITHDRAWAL" else "PURCHASE",
      channel = if (category == "ATM") "ATM" else channels(random.nextInt(channels.length)),
      location = locations(random.nextInt(locations.length)),
      deviceId = Some(s"DEV${random.nextInt(5000)}"),
      ipAddress = Some(s"${random.nextInt(256)}.${random.nextInt(256)}.${random.nextInt(256)}.${random.nextInt(256)}"),
      timestamp = adjustedTimestamp,
      cardType = cardTypes(random.nextInt(cardTypes.length)),
      isInternational = isIntl,
      merchantCategory = category,
      previousBalance = random.nextDouble() * 10000 + 1000,
      isFraud = Some(true)
    )
  }

  /**
   * Générer une transaction (normale ou frauduleuse selon le taux de fraude)
   */
  def generateTransaction(fraudRate: Double = 0.02): Transaction = {
    val customerId = s"CUST${random.nextInt(10000)}"

    if (random.nextDouble() < fraudRate) {
      generateFraudulentTransaction(customerId)
    } else {
      generateNormalTransaction(customerId)
    }
  }

  /**
   * Envoyer une transaction à Kafka
   */
  def sendTransaction(
    producer: KafkaProducer[String, String],
    topic: String,
    transaction: Transaction
  ): Unit = {
    val json = Transaction.toJson(transaction)
    val record = new ProducerRecord[String, String](topic, transaction.transactionId, json)

    producer.send(record, new Callback {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
        if (exception != null) {
          println(s"Error sending transaction ${transaction.transactionId}: ${exception.getMessage}")
        }
      }
    })
  }

  /**
   * Démarrer la production continue de transactions
   */
  def startContinuousProduction(
    producer: KafkaProducer[String, String],
    topic: String,
    transactionsPerSecond: Int = 1000,
    fraudRate: Double = 0.02
  ): Unit = {
    val intervalNanos = 1000000000L / transactionsPerSecond
    var lastPrintTime = System.currentTimeMillis()
    var messageCount = 0L
    var fraudCount = 0L

    println(s"Starting production: $transactionsPerSecond TPS, fraud rate: ${fraudRate * 100}%")
    println("=" * 60)

    while (true) {
      val startTime = System.nanoTime()

      // Générer et envoyer une transaction
      val transaction = generateTransaction(fraudRate)
      sendTransaction(producer, topic, transaction)

      messageCount += 1
      if (transaction.isFraud.getOrElse(false)) fraudCount += 1

      // Afficher les statistiques toutes les secondes
      val currentTime = System.currentTimeMillis()
      if (currentTime - lastPrintTime >= 1000) {
        val elapsedSeconds = (currentTime - lastPrintTime) / 1000.0
        val actualTps = messageCount / elapsedSeconds
        println(f"[${java.time.LocalTime.now()}] Messages: $messageCount%,d | " +
          f"Frauds: $fraudCount%,d | TPS: $actualTps%.0f")
        messageCount = 0
        fraudCount = 0
        lastPrintTime = currentTime
      }

      // Contrôle du débit
      val elapsedNanos = System.nanoTime() - startTime
      val sleepNanos = intervalNanos - elapsedNanos
      if (sleepNanos > 0) {
        Thread.sleep(sleepNanos / 1000000, (sleepNanos % 1000000).toInt)
      }
    }
  }

  /**
   * Production en mode burst (pour tests de charge)
   */
  def burstProduction(
    producer: KafkaProducer[String, String],
    topic: String,
    messageCount: Int,
    fraudRate: Double = 0.02
  ): Long = {
    val startTime = System.currentTimeMillis()
    var fraudCount = 0

    (1 to messageCount).foreach { i =>
      val transaction = generateTransaction(fraudRate)
      sendTransaction(producer, topic, transaction)
      if (transaction.isFraud.getOrElse(false)) fraudCount += 1

      if (i % 10000 == 0) {
        println(s"Sent $i / $messageCount messages...")
      }
    }

    producer.flush()
    val endTime = System.currentTimeMillis()
    val duration = endTime - startTime
    val tps = messageCount * 1000.0 / duration

    println("=" * 60)
    println(f"Burst production completed:")
    println(f"  Total messages: $messageCount%,d")
    println(f"  Fraud messages: $fraudCount%,d (${fraudCount * 100.0 / messageCount}%.2f%%)")
    println(f"  Duration: ${duration}ms")
    println(f"  Throughput: $tps%.0f TPS")
    println("=" * 60)

    duration
  }

  /**
   * Point d'entrée principal
   */
  def main(args: Array[String]): Unit = {
    println("=" * 60)
    println("Kafka Transaction Producer")
    println("=" * 60)

    val mode = if (args.length > 0) args(0) else "continuous"
    val topic = ConfigManager.Kafka.transactionsTopic
    val producer = createProducer()

    println(s"Mode: $mode")
    println(s"Topic: $topic")
    println(s"Bootstrap Servers: ${ConfigManager.Kafka.bootstrapServers}")
    println("=" * 60)

    try {
      mode match {
        case "continuous" =>
          val tps = if (args.length > 1) args(1).toInt else 1000
          val fraudRate = if (args.length > 2) args(2).toDouble else 0.02
          startContinuousProduction(producer, topic, tps, fraudRate)

        case "burst" =>
          val count = if (args.length > 1) args(1).toInt else 100000
          val fraudRate = if (args.length > 2) args(2).toDouble else 0.02
          burstProduction(producer, topic, count, fraudRate)

        case "single" =>
          val tx = generateTransaction(0.5) // 50% chance de fraude pour test
          sendTransaction(producer, topic, tx)
          producer.flush()
          println(s"Sent single transaction: ${Transaction.toJson(tx)}")

        case _ =>
          println(s"Unknown mode: $mode")
          println("Usage: TransactionProducer [continuous|burst|single] [tps/count] [fraudRate]")
      }
    } finally {
      producer.close()
    }
  }
}
