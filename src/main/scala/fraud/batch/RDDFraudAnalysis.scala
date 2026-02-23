package fraud.batch

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import fraud.models.Transaction
import fraud.utils.SparkSessionWrapper

/**
 * RDDFraudAnalysis - Analyse de fraude utilisant les RDD Spark
 *
 * Cette classe démontre l'utilisation des RDD (Resilient Distributed Datasets)
 * pour le traitement distribué des transactions financières.
 *
 * Concepts RDD illustrés:
 * - Transformations: map, filter, flatMap, reduceByKey, groupByKey
 * - Actions: count, collect, reduce, take, saveAsTextFile
 * - Persistence: cache, persist
 * - Partitioning: repartition, coalesce
 */
object RDDFraudAnalysis extends SparkSessionWrapper {

  /**
   * Charge les transactions depuis un fichier texte
   * Démonstration: Création de RDD et transformation map
   */
  def loadTransactionsRDD(sc: SparkContext, path: String): RDD[Transaction] = {
    sc.textFile(path)
      .map(line => parseTransaction(line))
      .filter(_.isSuccess)
      .map(_.get)
  }

  /**
   * Parse une ligne CSV en Transaction
   */
  private def parseTransaction(line: String): scala.util.Try[Transaction] = {
    scala.util.Try {
      val parts = line.split(",")
      Transaction(
        transactionId = parts(0),
        customerId = parts(1),
        merchantId = parts(2),
        amount = parts(3).toDouble,
        currency = parts(4),
        transactionType = parts(5),
        channel = parts(6),
        location = parts(7),
        deviceId = if (parts(8).nonEmpty) Some(parts(8)) else None,
        ipAddress = if (parts(9).nonEmpty) Some(parts(9)) else None,
        timestamp = parts(10).toLong,
        cardType = parts(11),
        isInternational = parts(12).toBoolean,
        merchantCategory = parts(13),
        previousBalance = parts(14).toDouble,
        isFraud = if (parts.length > 15) Some(parts(15).toBoolean) else None
      )
    }
  }

  // ==========================================================================
  // TRANSFORMATIONS RDD - Exemples fondamentaux
  // ==========================================================================

  /**
   * Filter - Filtrer les transactions de montant élevé
   * Transformation: Sélectionne uniquement les éléments satisfaisant une condition
   */
  def filterHighValueTransactions(
    transactions: RDD[Transaction],
    threshold: Double
  ): RDD[Transaction] = {
    transactions.filter(tx => tx.amount > threshold)
  }

  /**
   * Map - Extraire les montants des transactions
   * Transformation: Applique une fonction à chaque élément
   */
  def extractAmounts(transactions: RDD[Transaction]): RDD[Double] = {
    transactions.map(tx => tx.amount)
  }

  /**
   * FlatMap - Extraire toutes les localisations uniques par client
   * Transformation: Applique une fonction qui retourne une séquence et aplatit le résultat
   */
  def extractCustomerLocations(
    transactions: RDD[Transaction]
  ): RDD[(String, String)] = {
    transactions.flatMap(tx => Seq((tx.customerId, tx.location)))
  }

  /**
   * ReduceByKey - Calculer le montant total par client
   * Transformation: Agrège les valeurs par clé
   */
  def totalAmountByCustomer(
    transactions: RDD[Transaction]
  ): RDD[(String, Double)] = {
    transactions
      .map(tx => (tx.customerId, tx.amount))
      .reduceByKey(_ + _)
  }

  /**
   * GroupByKey - Grouper les transactions par client
   * Transformation: Groupe toutes les valeurs par clé
   */
  def groupTransactionsByCustomer(
    transactions: RDD[Transaction]
  ): RDD[(String, Iterable[Transaction])] = {
    transactions
      .map(tx => (tx.customerId, tx))
      .groupByKey()
  }

  /**
   * AggregateByKey - Statistiques complexes par marchand
   * Transformation: Agrégation avec valeur initiale et fonctions de combinaison
   */
  def merchantStatistics(
    transactions: RDD[Transaction]
  ): RDD[(String, (Double, Double, Long))] = {
    // (sum, max, count)
    val zero = (0.0, 0.0, 0L)

    transactions
      .map(tx => (tx.merchantId, tx.amount))
      .aggregateByKey(zero)(
        // Combiner dans une partition
        (acc, amount) => (acc._1 + amount, math.max(acc._2, amount), acc._3 + 1),
        // Combiner entre partitions
        (acc1, acc2) => (acc1._1 + acc2._1, math.max(acc1._2, acc2._2), acc1._3 + acc2._3)
      )
  }

  // ==========================================================================
  // DÉTECTION DE FRAUDE AVEC RDD
  // ==========================================================================

  /**
   * Détecter les transactions potentiellement frauduleuses
   * Utilise des règles simples basées sur les patterns
   */
  def detectSuspiciousTransactions(
    transactions: RDD[Transaction]
  ): RDD[(Transaction, Seq[String])] = {

    transactions.map { tx =>
      val reasons = scala.collection.mutable.ArrayBuffer[String]()

      // Règle 1: Montant élevé
      if (tx.amount > 5000) {
        reasons += s"HIGH_AMOUNT: ${tx.amount}"
      }

      // Règle 2: Transaction internationale avec montant élevé
      if (tx.isInternational && tx.amount > 1000) {
        reasons += "HIGH_INTERNATIONAL_TX"
      }

      // Règle 3: Transaction de nuit (heure suspecte)
      val hour = (tx.timestamp / 3600000) % 24
      if (hour >= 0 && hour <= 5) {
        reasons += s"SUSPICIOUS_HOUR: $hour"
      }

      // Règle 4: Catégorie marchande à risque
      val riskyCategories = Set("GAMBLING", "CRYPTO", "WIRE_TRANSFER")
      if (riskyCategories.contains(tx.merchantCategory)) {
        reasons += s"RISKY_MERCHANT_CATEGORY: ${tx.merchantCategory}"
      }

      (tx, reasons.toSeq)
    }.filter(_._2.nonEmpty)
  }

  /**
   * Analyse de vélocité par client
   * Calcule le nombre de transactions par fenêtre de temps
   */
  def velocityAnalysis(
    transactions: RDD[Transaction],
    windowSizeMs: Long
  ): RDD[(String, Int)] = {
    transactions
      .map(tx => ((tx.customerId, tx.timestamp / windowSizeMs), 1))
      .reduceByKey(_ + _)
      .map { case ((customerId, _), count) => (customerId, count) }
      .reduceByKey(math.max)
  }

  /**
   * Calcul du Z-score des montants par client
   * Identifie les transactions avec des montants anormaux
   */
  def calculateAmountZScores(
    transactions: RDD[Transaction]
  ): RDD[(Transaction, Double)] = {

    // Calculer moyenne et écart-type par client
    val customerStats: RDD[(String, (Double, Double))] = transactions
      .map(tx => (tx.customerId, (tx.amount, 1L)))
      .reduceByKey { case ((sum1, cnt1), (sum2, cnt2)) =>
        (sum1 + sum2, cnt1 + cnt2)
      }
      .mapValues { case (sum, cnt) => sum / cnt }
      .join(
        transactions
          .map(tx => (tx.customerId, tx.amount))
          .groupByKey()
          .mapValues { amounts =>
            val list = amounts.toList
            val mean = list.sum / list.size
            val variance = list.map(a => math.pow(a - mean, 2)).sum / list.size
            (mean, math.sqrt(variance))
          }
      )
      .mapValues(_._2)

    // Joindre avec les transactions et calculer z-score
    transactions
      .map(tx => (tx.customerId, tx))
      .join(customerStats)
      .map { case (_, (tx, (mean, std))) =>
        val zScore = if (std > 0) (tx.amount - mean) / std else 0.0
        (tx, zScore)
      }
  }

  // ==========================================================================
  // ACTIONS RDD
  // ==========================================================================

  /**
   * Calculer les statistiques globales
   * Action: Ramène les résultats au driver
   */
  def computeGlobalStats(transactions: RDD[Transaction]): Map[String, Any] = {
    // Cache pour éviter de recalculer
    transactions.cache()

    val count = transactions.count()
    val totalAmount = transactions.map(_.amount).sum()
    val avgAmount = totalAmount / count
    val maxAmount = transactions.map(_.amount).max()
    val minAmount = transactions.map(_.amount).min()

    val fraudCount = transactions.filter(_.isFraud.getOrElse(false)).count()

    transactions.unpersist()

    Map(
      "totalTransactions" -> count,
      "totalAmount" -> totalAmount,
      "avgAmount" -> avgAmount,
      "maxAmount" -> maxAmount,
      "minAmount" -> minAmount,
      "fraudCount" -> fraudCount,
      "fraudRate" -> (fraudCount.toDouble / count)
    )
  }

  /**
   * Top N clients par montant total
   * Action: Collecte les N premiers résultats
   */
  def topCustomersByAmount(
    transactions: RDD[Transaction],
    n: Int
  ): Array[(String, Double)] = {
    totalAmountByCustomer(transactions)
      .sortBy(_._2, ascending = false)
      .take(n)
  }

  /**
   * Sauvegarder les transactions suspectes dans HDFS
   * Action: Écrit les données dans un système de fichiers distribué
   */
  def saveSuspiciousTransactions(
    suspiciousTx: RDD[(Transaction, Seq[String])],
    outputPath: String
  ): Unit = {
    suspiciousTx
      .map { case (tx, reasons) =>
        s"${tx.transactionId},${tx.customerId},${tx.amount},${reasons.mkString(";")}"
      }
      .saveAsTextFile(outputPath)
  }

  // ==========================================================================
  // PARTITIONING ET OPTIMISATION
  // ==========================================================================

  /**
   * Repartitionner les données par client ID
   * Optimise les jointures et agrégations par client
   */
  def partitionByCustomer(
    transactions: RDD[Transaction],
    numPartitions: Int
  ): RDD[(String, Transaction)] = {
    transactions
      .map(tx => (tx.customerId, tx))
      .partitionBy(new org.apache.spark.HashPartitioner(numPartitions))
  }

  /**
   * Coalesce pour réduire le nombre de partitions
   * Utile après des filtres qui réduisent significativement les données
   */
  def optimizePartitions(
    transactions: RDD[Transaction],
    targetPartitions: Int
  ): RDD[Transaction] = {
    transactions.coalesce(targetPartitions)
  }

  /**
   * Point d'entrée principal pour démonstration
   */
  def main(args: Array[String]): Unit = {
    val spark = localSparkSession("RDDFraudAnalysis")
    val sc = spark.sparkContext

    println("=" * 60)
    println("RDD Fraud Analysis - Démonstration")
    println("=" * 60)

    // Créer des données de test
    val testTransactions = sc.parallelize(generateSampleTransactions(1000))

    // Exécuter les analyses
    println("\n1. Statistiques globales:")
    val stats = computeGlobalStats(testTransactions)
    stats.foreach { case (k, v) => println(s"   $k: $v") }

    println("\n2. Top 5 clients par montant:")
    topCustomersByAmount(testTransactions, 5).foreach { case (cust, amt) =>
      println(s"   $cust: $$${amt}")
    }

    println("\n3. Transactions suspectes détectées:")
    val suspicious = detectSuspiciousTransactions(testTransactions)
    println(s"   Nombre: ${suspicious.count()}")

    spark.stop()
  }

  /**
   * Génère des transactions de test
   */
  private def generateSampleTransactions(n: Int): Seq[Transaction] = {
    val random = new scala.util.Random(42)
    val locations = Seq("Paris", "London", "New York", "Tokyo", "Singapore")
    val merchants = Seq("RETAIL", "GROCERY", "RESTAURANT", "ONLINE", "GAMBLING")
    val channels = Seq("ONLINE", "POS", "ATM")

    (1 to n).map { i =>
      val isFraud = random.nextDouble() < 0.02 // 2% fraud rate
      val amount = if (isFraud) random.nextDouble() * 10000 + 1000
                   else random.nextDouble() * 500 + 10

      Transaction(
        transactionId = s"TX$i",
        customerId = s"CUST${random.nextInt(100)}",
        merchantId = s"MERCH${random.nextInt(50)}",
        amount = amount,
        currency = "EUR",
        transactionType = "PURCHASE",
        channel = channels(random.nextInt(channels.size)),
        location = locations(random.nextInt(locations.size)),
        deviceId = Some(s"DEV${random.nextInt(200)}"),
        ipAddress = Some(s"192.168.${random.nextInt(256)}.${random.nextInt(256)}"),
        timestamp = System.currentTimeMillis() - random.nextInt(86400000),
        cardType = if (random.nextBoolean()) "CREDIT" else "DEBIT",
        isInternational = random.nextDouble() < 0.1,
        merchantCategory = merchants(random.nextInt(merchants.size)),
        previousBalance = random.nextDouble() * 10000,
        isFraud = Some(isFraud)
      )
    }
  }
}
