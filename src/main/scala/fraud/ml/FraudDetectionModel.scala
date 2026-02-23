package fraud.ml

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature._
import org.apache.spark.ml.classification._
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import fraud.utils.{ConfigManager, SparkSessionWrapper}

/**
 * FraudDetectionModel - Modèle ML pour la détection de fraude
 *
 * Ce module implémente un pipeline ML complet avec Spark MLlib:
 * - Feature engineering automatisé
 * - Entraînement avec validation croisée
 * - Évaluation des performances (AUC, précision, recall)
 * - Sauvegarde et chargement du modèle
 *
 * Algorithmes supportés:
 * - Logistic Regression
 * - Random Forest
 * - Gradient Boosted Trees
 */
object FraudDetectionModel extends SparkSessionWrapper {

  // Colonnes catégorielles à encoder
  val categoricalCols: Array[String] = Array(
    "transactionType", "channel", "cardType", "merchantCategory", "currency"
  )

  // Colonnes numériques
  val numericCols: Array[String] = Array(
    "amount", "previousBalance", "hour", "dayOfWeek",
    "amountZScore", "velocityScore", "timeSinceLastTx"
  )

  /**
   * Préparer les features pour l'entraînement
   */
  def prepareFeatures(df: DataFrame): DataFrame = {
    df
      // Extraire les features temporelles
      .withColumn("hour", hour(from_unixtime(col("timestamp") / 1000)))
      .withColumn("dayOfWeek", dayofweek(from_unixtime(col("timestamp") / 1000)))
      // Convertir les booléens en numériques
      .withColumn("isInternationalNum", col("isInternational").cast("integer"))
      // Remplir les valeurs manquantes
      .na.fill(0.0, numericCols)
      .na.fill("UNKNOWN", categoricalCols)
      // S'assurer que le label est présent
      .withColumn("label", col("isFraud").cast("double"))
  }

  /**
   * Créer le pipeline de feature engineering
   */
  def createFeaturePipeline(): Pipeline = {
    // 1. Indexer les colonnes catégorielles
    val stringIndexers = categoricalCols.map { col =>
      new StringIndexer()
        .setInputCol(col)
        .setOutputCol(s"${col}Index")
        .setHandleInvalid("keep")
    }

    // 2. One-Hot Encoder pour les catégories
    val oneHotEncoder = new OneHotEncoder()
      .setInputCols(categoricalCols.map(_ + "Index"))
      .setOutputCols(categoricalCols.map(_ + "Vec"))

    // 3. Assembler les features numériques
    val numericAssembler = new VectorAssembler()
      .setInputCols(numericCols ++ Array("isInternationalNum"))
      .setOutputCol("numericFeatures")
      .setHandleInvalid("skip")

    // 4. Standardiser les features numériques
    val scaler = new StandardScaler()
      .setInputCol("numericFeatures")
      .setOutputCol("scaledNumericFeatures")
      .setWithStd(true)
      .setWithMean(true)

    // 5. Assembler toutes les features
    val finalAssembler = new VectorAssembler()
      .setInputCols(
        categoricalCols.map(_ + "Vec") ++ Array("scaledNumericFeatures")
      )
      .setOutputCol("features")
      .setHandleInvalid("skip")

    new Pipeline().setStages(
      stringIndexers ++ Array(oneHotEncoder, numericAssembler, scaler, finalAssembler)
    )
  }

  /**
   * Créer et entraîner un modèle de Régression Logistique
   */
  def trainLogisticRegression(
    trainingData: DataFrame,
    maxIter: Int = ConfigManager.MLModel.maxIterations,
    regParam: Double = ConfigManager.MLModel.regParam,
    elasticNetParam: Double = ConfigManager.MLModel.elasticNetParam
  ): PipelineModel = {

    println("Training Logistic Regression model...")

    // Préparer les données
    val preparedData = prepareFeatures(trainingData)

    // Pipeline de features
    val featurePipeline = createFeaturePipeline()

    // Modèle de classification
    val lr = new LogisticRegression()
      .setMaxIter(maxIter)
      .setRegParam(regParam)
      .setElasticNetParam(elasticNetParam)
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setProbabilityCol("probability")
      .setRawPredictionCol("rawPrediction")

    // Pipeline complet
    val pipeline = new Pipeline()
      .setStages(featurePipeline.getStages ++ Array(lr))

    // Entraîner
    val model = pipeline.fit(preparedData)

    println("Logistic Regression model trained successfully!")
    model
  }

  /**
   * Créer et entraîner un modèle Random Forest
   */
  def trainRandomForest(
    trainingData: DataFrame,
    numTrees: Int = 100,
    maxDepth: Int = 10
  ): PipelineModel = {

    println("Training Random Forest model...")

    val preparedData = prepareFeatures(trainingData)
    val featurePipeline = createFeaturePipeline()

    val rf = new RandomForestClassifier()
      .setNumTrees(numTrees)
      .setMaxDepth(maxDepth)
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setProbabilityCol("probability")
      .setRawPredictionCol("rawPrediction")
      .setSeed(42)

    val pipeline = new Pipeline()
      .setStages(featurePipeline.getStages ++ Array(rf))

    val model = pipeline.fit(preparedData)

    println("Random Forest model trained successfully!")
    model
  }

  /**
   * Créer et entraîner un modèle Gradient Boosted Trees
   */
  def trainGBT(
    trainingData: DataFrame,
    maxIter: Int = 50,
    maxDepth: Int = 5
  ): PipelineModel = {

    println("Training Gradient Boosted Trees model...")

    val preparedData = prepareFeatures(trainingData)
    val featurePipeline = createFeaturePipeline()

    val gbt = new GBTClassifier()
      .setMaxIter(maxIter)
      .setMaxDepth(maxDepth)
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setSeed(42)

    val pipeline = new Pipeline()
      .setStages(featurePipeline.getStages ++ Array(gbt))

    val model = pipeline.fit(preparedData)

    println("GBT model trained successfully!")
    model
  }

  /**
   * Entraîner avec validation croisée et recherche d'hyperparamètres
   */
  def trainWithCrossValidation(
    trainingData: DataFrame,
    numFolds: Int = 5
  ): PipelineModel = {

    println(s"Training with $numFolds-fold cross-validation...")

    val preparedData = prepareFeatures(trainingData)
    val featurePipeline = createFeaturePipeline()

    val lr = new LogisticRegression()
      .setFeaturesCol("features")
      .setLabelCol("label")

    val pipeline = new Pipeline()
      .setStages(featurePipeline.getStages ++ Array(lr))

    // Grille de paramètres
    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.maxIter, Array(50, 100))
      .addGrid(lr.regParam, Array(0.01, 0.1))
      .addGrid(lr.elasticNetParam, Array(0.0, 0.5, 1.0))
      .build()

    // Évaluateur
    val evaluator = new BinaryClassificationEvaluator()
      .setLabelCol("label")
      .setRawPredictionCol("rawPrediction")
      .setMetricName("areaUnderROC")

    // Validation croisée
    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(numFolds)
      .setParallelism(4)

    val cvModel = cv.fit(preparedData)

    println(s"Best model AUC: ${cvModel.avgMetrics.max}")
    cvModel.bestModel.asInstanceOf[PipelineModel]
  }

  /**
   * Évaluer les performances du modèle
   */
  def evaluateModel(
    model: PipelineModel,
    testData: DataFrame
  ): Map[String, Double] = {

    val preparedData = prepareFeatures(testData)
    val predictions = model.transform(preparedData)

    // Évaluateur binaire
    val evaluator = new BinaryClassificationEvaluator()
      .setLabelCol("label")
      .setRawPredictionCol("rawPrediction")

    // Calcul des métriques
    val auc = evaluator.setMetricName("areaUnderROC").evaluate(predictions)
    val auPR = evaluator.setMetricName("areaUnderPR").evaluate(predictions)

    // Calcul précision, recall, F1
    val tp = predictions.filter(col("prediction") === 1.0 && col("label") === 1.0).count().toDouble
    val fp = predictions.filter(col("prediction") === 1.0 && col("label") === 0.0).count().toDouble
    val fn = predictions.filter(col("prediction") === 0.0 && col("label") === 1.0).count().toDouble
    val tn = predictions.filter(col("prediction") === 0.0 && col("label") === 0.0).count().toDouble

    val precision = if (tp + fp > 0) tp / (tp + fp) else 0.0
    val recall = if (tp + fn > 0) tp / (tp + fn) else 0.0
    val f1 = if (precision + recall > 0) 2 * precision * recall / (precision + recall) else 0.0
    val accuracy = (tp + tn) / (tp + tn + fp + fn)

    Map(
      "auc" -> auc,
      "auPR" -> auPR,
      "accuracy" -> accuracy,
      "precision" -> precision,
      "recall" -> recall,
      "f1" -> f1,
      "truePositives" -> tp,
      "falsePositives" -> fp,
      "trueNegatives" -> tn,
      "falseNegatives" -> fn
    )
  }

  /**
   * Afficher le rapport d'évaluation
   */
  def printEvaluationReport(metrics: Map[String, Double]): Unit = {
    println("\n" + "=" * 60)
    println("MODEL EVALUATION REPORT")
    println("=" * 60)
    println(f"AUC-ROC:       ${metrics("auc")}%.4f")
    println(f"AUC-PR:        ${metrics("auPR")}%.4f")
    println(f"Accuracy:      ${metrics("accuracy")}%.4f")
    println(f"Precision:     ${metrics("precision")}%.4f")
    println(f"Recall:        ${metrics("recall")}%.4f")
    println(f"F1-Score:      ${metrics("f1")}%.4f")
    println("-" * 60)
    println("Confusion Matrix:")
    println(f"  TP: ${metrics("truePositives")}%.0f | FP: ${metrics("falsePositives")}%.0f")
    println(f"  FN: ${metrics("falseNegatives")}%.0f | TN: ${metrics("trueNegatives")}%.0f")
    println("=" * 60)
  }

  /**
   * Sauvegarder le modèle
   */
  def saveModel(model: PipelineModel, path: String): Unit = {
    model.write.overwrite().save(path)
    println(s"Model saved to: $path")
  }

  /**
   * Charger un modèle sauvegardé
   */
  def loadModel(path: String): PipelineModel = {
    val model = PipelineModel.load(path)
    println(s"Model loaded from: $path")
    model
  }

  /**
   * Faire des prédictions sur de nouvelles données
   */
  def predict(
    model: PipelineModel,
    transactions: DataFrame,
    threshold: Double = ConfigManager.MLModel.fraudThreshold
  ): DataFrame = {

    val preparedData = prepareFeatures(transactions)
    val predictions = model.transform(preparedData)

    // Extraire la probabilité de fraude et appliquer le seuil
    val getFraudProb = udf((probability: org.apache.spark.ml.linalg.Vector) =>
      probability(1)
    )

    predictions
      .withColumn("fraudProbability", getFraudProb(col("probability")))
      .withColumn("isFraudPredicted", col("fraudProbability") >= threshold)
      .withColumn("riskLevel",
        when(col("fraudProbability") >= 0.9, "CRITICAL")
          .when(col("fraudProbability") >= 0.7, "HIGH")
          .when(col("fraudProbability") >= 0.5, "MEDIUM")
          .otherwise("LOW")
      )
      .select(
        col("transactionId"),
        col("customerId"),
        col("amount"),
        col("fraudProbability"),
        col("isFraudPredicted"),
        col("riskLevel")
      )
  }

  /**
   * Point d'entrée principal pour démonstration
   */
  def main(args: Array[String]): Unit = {
    val spark = localSparkSession("FraudDetectionML")
    import spark.implicits._

    println("=" * 60)
    println("Fraud Detection ML Model - Training & Evaluation")
    println("=" * 60)

    // Générer des données d'entraînement synthétiques
    println("\n1. Generating synthetic training data...")
    val transactions = generateTrainingData(spark, 50000, fraudRate = 0.03)
    println(s"   Total transactions: ${transactions.count()}")
    println(s"   Fraud transactions: ${transactions.filter(col("isFraud")).count()}")

    // Split train/test
    val Array(trainData, testData) = transactions.randomSplit(Array(0.8, 0.2), seed = 42)
    println(s"   Training set: ${trainData.count()}")
    println(s"   Test set: ${testData.count()}")

    // Entraîner différents modèles
    println("\n2. Training models...")

    // Logistic Regression
    val lrModel = trainLogisticRegression(trainData)
    val lrMetrics = evaluateModel(lrModel, testData)
    println("\n--- Logistic Regression ---")
    printEvaluationReport(lrMetrics)

    // Random Forest
    val rfModel = trainRandomForest(trainData, numTrees = 50)
    val rfMetrics = evaluateModel(rfModel, testData)
    println("\n--- Random Forest ---")
    printEvaluationReport(rfMetrics)

    // GBT
    val gbtModel = trainGBT(trainData)
    val gbtMetrics = evaluateModel(gbtModel, testData)
    println("\n--- Gradient Boosted Trees ---")
    printEvaluationReport(gbtMetrics)

    // Sélectionner le meilleur modèle
    val bestModel = Seq(
      ("LogisticRegression", lrModel, lrMetrics("f1")),
      ("RandomForest", rfModel, rfMetrics("f1")),
      ("GBT", gbtModel, gbtMetrics("f1"))
    ).maxBy(_._3)

    println(s"\n3. Best model: ${bestModel._1} (F1: ${bestModel._3})")

    // Sauvegarder le meilleur modèle
    saveModel(bestModel._2, "/tmp/fraud_detection_model")

    // Exemple de prédiction
    println("\n4. Sample predictions:")
    val samplePredictions = predict(bestModel._2, testData.limit(10))
    samplePredictions.show()

    spark.stop()
  }

  /**
   * Générer des données d'entraînement synthétiques
   */
  private def generateTrainingData(
    spark: SparkSession,
    count: Int,
    fraudRate: Double
  ): DataFrame = {
    import spark.implicits._
    import fraud.batch.RDDFraudAnalysis

    val transactions = (1 to count).map { _ =>
      val random = new scala.util.Random()
      val isFraud = random.nextDouble() < fraudRate

      if (isFraud) {
        // Transaction frauduleuse avec patterns distinctifs
        val amount = random.nextDouble() * 8000 + 2000
        fraud.models.Transaction(
          transactionId = s"TX${System.currentTimeMillis()}-${random.nextInt(10000)}",
          customerId = s"CUST${random.nextInt(1000)}",
          merchantId = s"MERCH${random.nextInt(100)}",
          amount = amount,
          currency = "EUR",
          transactionType = "PURCHASE",
          channel = if (random.nextBoolean()) "ONLINE" else "ATM",
          location = Seq("Paris", "London", "Unknown")(random.nextInt(3)),
          deviceId = Some(s"DEV${random.nextInt(100)}"),
          ipAddress = Some(s"${random.nextInt(256)}.${random.nextInt(256)}.${random.nextInt(256)}.${random.nextInt(256)}"),
          timestamp = System.currentTimeMillis() - random.nextInt(86400000),
          cardType = "CREDIT",
          isInternational = random.nextDouble() < 0.5,
          merchantCategory = Seq("GAMBLING", "CRYPTO", "WIRE_TRANSFER")(random.nextInt(3)),
          previousBalance = random.nextDouble() * 5000,
          isFraud = Some(true)
        )
      } else {
        // Transaction normale
        RDDFraudAnalysis.generateSampleTransactions(1).head.copy(isFraud = Some(false))
      }
    }

    transactions.toDF()
      // Ajouter des features dérivées
      .withColumn("amountZScore", lit(0.0))
      .withColumn("velocityScore", lit(0.0))
      .withColumn("timeSinceLastTx", lit(0L))
  }
}
