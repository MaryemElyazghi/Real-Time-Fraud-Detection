package fraud.models

import java.sql.Timestamp

/**
 * Transaction - Modèle de données pour les transactions financières
 *
 * Ce case class représente une transaction financière avec tous les attributs
 * nécessaires pour la détection de fraude.
 */
case class Transaction(
  transactionId: String,          // Identifiant unique de la transaction
  customerId: String,             // Identifiant du client
  merchantId: String,             // Identifiant du marchand
  amount: Double,                 // Montant de la transaction
  currency: String,               // Devise (EUR, USD, etc.)
  transactionType: String,        // Type: PURCHASE, WITHDRAWAL, TRANSFER
  channel: String,                // Canal: ONLINE, POS, ATM
  location: String,               // Localisation géographique
  deviceId: Option[String],       // Identifiant de l'appareil (si disponible)
  ipAddress: Option[String],      // Adresse IP (pour transactions en ligne)
  timestamp: Long,                // Timestamp Unix en millisecondes
  cardType: String,               // Type de carte: DEBIT, CREDIT
  isInternational: Boolean,       // Transaction internationale
  merchantCategory: String,       // Catégorie du marchand (MCC)
  previousBalance: Double,        // Solde avant transaction
  isFraud: Option[Boolean] = None // Label de fraude (pour entraînement ML)
)

/**
 * TransactionWithFeatures - Transaction enrichie avec features ML
 */
case class TransactionWithFeatures(
  transaction: Transaction,
  // Features calculées
  hourOfDay: Int,                 // Heure de la transaction (0-23)
  dayOfWeek: Int,                 // Jour de la semaine (1-7)
  amountZScore: Double,           // Z-score du montant
  velocityScore: Double,          // Score de vélocité (transactions/heure)
  distanceFromLastTx: Double,     // Distance depuis dernière transaction
  timeSinceLastTx: Long,          // Temps depuis dernière transaction (ms)
  avgAmountRatio: Double,         // Ratio montant/moyenne historique
  isHighRisk: Boolean             // Flag de risque élevé
)

/**
 * FraudPrediction - Résultat de prédiction de fraude
 */
case class FraudPrediction(
  transactionId: String,
  customerId: String,
  fraudProbability: Double,       // Probabilité de fraude (0-1)
  isFraud: Boolean,               // Prédiction binaire
  riskLevel: String,              // LOW, MEDIUM, HIGH, CRITICAL
  reasons: Seq[String],           // Raisons de la classification
  processedAt: Long               // Timestamp de traitement
)

/**
 * CustomerProfile - Profil client pour analyse comportementale
 */
case class CustomerProfile(
  customerId: String,
  avgTransactionAmount: Double,
  stdTransactionAmount: Double,
  avgTransactionsPerDay: Double,
  preferredLocations: Seq[String],
  preferredMerchants: Seq[String],
  lastTransactionTime: Long,
  totalTransactions: Long,
  fraudCount: Int
)

/**
 * AlertEvent - Alerte générée pour transaction suspecte
 */
case class AlertEvent(
  alertId: String,
  transactionId: String,
  customerId: String,
  alertLevel: String,             // WARNING, CRITICAL
  alertType: String,              // UNUSUAL_AMOUNT, VELOCITY, LOCATION, etc.
  description: String,
  createdAt: Long,
  status: String                  // OPEN, INVESTIGATING, CLOSED
)

/**
 * Companion objects pour sérialisation JSON
 */
object Transaction {
  import io.circe._
  import io.circe.generic.semiauto._

  implicit val encoder: Encoder[Transaction] = deriveEncoder[Transaction]
  implicit val decoder: Decoder[Transaction] = deriveDecoder[Transaction]

  def fromJson(json: String): Either[Error, Transaction] = {
    io.circe.parser.decode[Transaction](json)
  }

  def toJson(tx: Transaction): String = {
    import io.circe.syntax._
    tx.asJson.noSpaces
  }
}

object FraudPrediction {
  import io.circe._
  import io.circe.generic.semiauto._

  implicit val encoder: Encoder[FraudPrediction] = deriveEncoder[FraudPrediction]
  implicit val decoder: Decoder[FraudPrediction] = deriveDecoder[FraudPrediction]

  def toJson(prediction: FraudPrediction): String = {
    import io.circe.syntax._
    prediction.asJson.noSpaces
  }
}

object AlertEvent {
  import io.circe._
  import io.circe.generic.semiauto._

  implicit val encoder: Encoder[AlertEvent] = deriveEncoder[AlertEvent]
  implicit val decoder: Decoder[AlertEvent] = deriveDecoder[AlertEvent]

  def toJson(alert: AlertEvent): String = {
    import io.circe.syntax._
    alert.asJson.noSpaces
  }
}
