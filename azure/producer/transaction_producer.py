#!/usr/bin/env python3
"""
Azure Event Hubs Transaction Producer
=====================================
Envoie des transactions simulées vers Azure Event Hubs (Kafka-compatible)

Usage:
    python transaction_producer.py --count 10000 --fraud-rate 0.03

Prérequis:
    pip install azure-eventhub python-dotenv
"""

import os
import json
import time
import random
import argparse
from datetime import datetime
from typing import List, Dict
from dotenv import load_dotenv

from azure.eventhub import EventHubProducerClient, EventData

# Charger les variables d'environnement
load_dotenv()


class TransactionGenerator:
    """Génère des transactions bancaires réalistes"""

    LOCATIONS = [
        {"city": "Paris", "country": "FR", "timezone": "Europe/Paris"},
        {"city": "London", "country": "GB", "timezone": "Europe/London"},
        {"city": "New York", "country": "US", "timezone": "America/New_York"},
        {"city": "Tokyo", "country": "JP", "timezone": "Asia/Tokyo"},
        {"city": "Singapore", "country": "SG", "timezone": "Asia/Singapore"},
        {"city": "Dubai", "country": "AE", "timezone": "Asia/Dubai"},
        {"city": "Sydney", "country": "AU", "timezone": "Australia/Sydney"},
        {"city": "Berlin", "country": "DE", "timezone": "Europe/Berlin"},
    ]

    MERCHANT_CATEGORIES = [
        {"code": "GROCERY", "avg_amount": 85, "std": 45, "fraud_risk": 0.01},
        {"code": "RESTAURANT", "avg_amount": 45, "std": 30, "fraud_risk": 0.02},
        {"code": "RETAIL", "avg_amount": 150, "std": 100, "fraud_risk": 0.03},
        {"code": "ONLINE", "avg_amount": 120, "std": 80, "fraud_risk": 0.05},
        {"code": "TRAVEL", "avg_amount": 450, "std": 300, "fraud_risk": 0.04},
        {"code": "ENTERTAINMENT", "avg_amount": 65, "std": 40, "fraud_risk": 0.02},
        {"code": "UTILITIES", "avg_amount": 120, "std": 50, "fraud_risk": 0.01},
        {"code": "HEALTHCARE", "avg_amount": 200, "std": 150, "fraud_risk": 0.01},
        {"code": "GAMBLING", "avg_amount": 500, "std": 400, "fraud_risk": 0.15},
        {"code": "CRYPTO", "avg_amount": 1000, "std": 800, "fraud_risk": 0.20},
    ]

    CHANNELS = ["ONLINE", "POS", "ATM", "MOBILE", "PHONE"]
    CARD_TYPES = ["CREDIT", "DEBIT", "PREPAID"]
    CURRENCIES = ["EUR", "USD", "GBP", "JPY", "SGD"]

    def __init__(self, num_customers: int = 5000, num_merchants: int = 500):
        self.num_customers = num_customers
        self.num_merchants = num_merchants
        self.tx_counter = 0

        # Pré-générer des profils clients
        self.customer_profiles = self._generate_customer_profiles()

    def _generate_customer_profiles(self) -> Dict:
        """Génère des profils clients avec des comportements différents"""
        profiles = {}
        for i in range(self.num_customers):
            customer_id = f"CUST{i:06d}"
            profiles[customer_id] = {
                "avg_monthly_spend": random.uniform(500, 5000),
                "preferred_location": random.choice(self.LOCATIONS),
                "preferred_channel": random.choice(self.CHANNELS[:3]),
                "risk_score": random.uniform(0, 0.3),
            }
        return profiles

    def generate_normal_transaction(self) -> Dict:
        """Génère une transaction normale"""
        self.tx_counter += 1

        # Sélectionner client et marchand
        customer_id = f"CUST{random.randint(0, self.num_customers-1):06d}"
        merchant_id = f"MERCH{random.randint(0, self.num_merchants-1):05d}"

        # Sélectionner catégorie marchand
        category = random.choice(self.MERCHANT_CATEGORIES[:8])  # Exclure GAMBLING/CRYPTO pour normal

        # Générer montant basé sur la catégorie
        amount = random.gauss(category["avg_amount"], category["std"])
        amount = max(1.0, amount)  # Minimum $1

        # Localisation
        location = random.choice(self.LOCATIONS)
        customer_location = self.customer_profiles[customer_id]["preferred_location"]
        is_international = location["country"] != customer_location["country"]

        # Timestamp
        timestamp = int(time.time() * 1000)

        return {
            "transactionId": f"TX{timestamp}-{self.tx_counter:08d}",
            "customerId": customer_id,
            "merchantId": merchant_id,
            "amount": round(amount, 2),
            "currency": random.choice(self.CURRENCIES[:3]),
            "channel": random.choice(self.CHANNELS),
            "location": location["city"],
            "country": location["country"],
            "timestamp": timestamp,
            "timestampStr": datetime.utcnow().isoformat() + "Z",
            "cardType": random.choice(self.CARD_TYPES),
            "isInternational": is_international,
            "merchantCategory": category["code"],
            "previousBalance": round(random.uniform(100, 15000), 2),
            "isFraud": False,
            "fraudType": None
        }

    def generate_fraudulent_transaction(self) -> Dict:
        """Génère une transaction frauduleuse"""
        tx = self.generate_normal_transaction()
        tx["isFraud"] = True

        # Type de fraude
        fraud_type = random.choice([
            "high_amount",
            "risky_merchant",
            "velocity_abuse",
            "geo_anomaly",
            "card_testing"
        ])

        tx["fraudType"] = fraud_type

        if fraud_type == "high_amount":
            # Montant anormalement élevé
            tx["amount"] = round(random.uniform(5000, 20000), 2)

        elif fraud_type == "risky_merchant":
            # Marchand à risque
            tx["merchantCategory"] = random.choice(["GAMBLING", "CRYPTO"])
            tx["amount"] = round(random.uniform(1000, 8000), 2)

        elif fraud_type == "velocity_abuse":
            # Plusieurs transactions rapides (on simule avec un flag)
            tx["amount"] = round(random.uniform(100, 500), 2)
            tx["velocityFlag"] = True

        elif fraud_type == "geo_anomaly":
            # Transaction depuis un pays inhabituel
            tx["isInternational"] = True
            tx["country"] = random.choice(["NG", "RU", "CN", "BR"])
            tx["amount"] = round(random.uniform(500, 3000), 2)

        elif fraud_type == "card_testing":
            # Petits montants pour tester la carte
            tx["amount"] = round(random.uniform(0.50, 5.00), 2)
            tx["channel"] = "ONLINE"

        return tx

    def generate_batch(self, count: int, fraud_rate: float = 0.03) -> List[Dict]:
        """Génère un batch de transactions"""
        transactions = []
        fraud_count = 0

        for _ in range(count):
            if random.random() < fraud_rate:
                tx = self.generate_fraudulent_transaction()
                fraud_count += 1
            else:
                tx = self.generate_normal_transaction()
            transactions.append(tx)

        return transactions, fraud_count


class EventHubsProducer:
    """Producteur Azure Event Hubs"""

    def __init__(self, connection_string: str, eventhub_name: str):
        self.connection_string = connection_string
        self.eventhub_name = eventhub_name
        self.producer = None

    def connect(self):
        """Établir la connexion"""
        self.producer = EventHubProducerClient.from_connection_string(
            conn_str=self.connection_string,
            eventhub_name=self.eventhub_name
        )
        print(f"✅ Connecté à Event Hub: {self.eventhub_name}")

    def send_batch(self, transactions: List[Dict]) -> int:
        """Envoyer un batch de transactions"""
        event_data_batch = self.producer.create_batch()
        sent_count = 0

        for tx in transactions:
            try:
                # Créer l'événement avec la clé de partition
                event = EventData(json.dumps(tx))
                event.properties = {
                    "customerId": tx["customerId"],
                    "merchantCategory": tx["merchantCategory"]
                }
                event_data_batch.add(event)
                sent_count += 1
            except ValueError:
                # Batch plein, envoyer et créer nouveau
                self.producer.send_batch(event_data_batch)
                event_data_batch = self.producer.create_batch()
                event_data_batch.add(EventData(json.dumps(tx)))
                sent_count += 1

        # Envoyer le dernier batch
        if sent_count > 0:
            self.producer.send_batch(event_data_batch)

        return sent_count

    def close(self):
        """Fermer la connexion"""
        if self.producer:
            self.producer.close()
            print("✅ Connexion fermée")


def main():
    parser = argparse.ArgumentParser(description="Azure Event Hubs Transaction Producer")
    parser.add_argument("--count", type=int, default=10000, help="Nombre de transactions")
    parser.add_argument("--fraud-rate", type=float, default=0.03, help="Taux de fraude (0.03 = 3%)")
    parser.add_argument("--batch-size", type=int, default=500, help="Taille des batches")
    parser.add_argument("--delay", type=float, default=0.1, help="Délai entre batches (secondes)")
    args = parser.parse_args()

    # Configuration
    connection_string = os.getenv("EVENT_HUB_CONNECTION_STRING")
    eventhub_name = os.getenv("EVENT_HUB_NAME", "transactions")

    if not connection_string:
        print("❌ ERROR: EVENT_HUB_CONNECTION_STRING non défini!")
        print("   Créez un fichier .env avec:")
        print('   EVENT_HUB_CONNECTION_STRING="Endpoint=sb://..."')
        return

    print("=" * 60)
    print("   AZURE EVENT HUBS - Transaction Producer")
    print("=" * 60)
    print(f"   Transactions: {args.count}")
    print(f"   Fraud rate:   {args.fraud_rate * 100:.1f}%")
    print(f"   Batch size:   {args.batch_size}")
    print("=" * 60)

    # Initialiser
    generator = TransactionGenerator()
    producer = EventHubsProducer(connection_string, eventhub_name)
    producer.connect()

    try:
        total_sent = 0
        total_frauds = 0
        start_time = time.time()

        while total_sent < args.count:
            # Calculer la taille du prochain batch
            remaining = args.count - total_sent
            batch_size = min(args.batch_size, remaining)

            # Générer et envoyer
            transactions, fraud_count = generator.generate_batch(batch_size, args.fraud_rate)
            sent = producer.send_batch(transactions)

            total_sent += sent
            total_frauds += fraud_count

            # Afficher progression
            elapsed = time.time() - start_time
            rate = total_sent / elapsed if elapsed > 0 else 0
            progress = total_sent / args.count * 100

            print(f"📤 Envoyé: {total_sent:,}/{args.count:,} ({progress:.1f}%) | "
                  f"Fraudes: {total_frauds} | Rate: {rate:.0f} tx/s")

            # Délai entre batches
            if args.delay > 0 and total_sent < args.count:
                time.sleep(args.delay)

        # Statistiques finales
        elapsed = time.time() - start_time
        print("\n" + "=" * 60)
        print("   TERMINÉ!")
        print("=" * 60)
        print(f"   ✅ Transactions envoyées: {total_sent:,}")
        print(f"   🚨 Fraudes:               {total_frauds} ({total_frauds/total_sent*100:.2f}%)")
        print(f"   ⏱️  Temps:                 {elapsed:.2f}s")
        print(f"   📊 Débit:                 {total_sent/elapsed:.0f} tx/s")
        print("=" * 60)

    except KeyboardInterrupt:
        print("\n⚠️ Interrupted by user")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
