# 🚀 Real-Time Fraud Detection on Azure

## Architecture Production

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         AZURE FRAUD DETECTION ARCHITECTURE                      │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│   ┌──────────────────┐                                                          │
│   │   Transaction    │                                                          │
│   │    Producer      │                                                          │
│   │    (Python)      │                                                          │
│   └────────┬─────────┘                                                          │
│            │                                                                    │
│            ▼                                                                    │
│   ┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐       │
│   │  Azure Event     │────▶│  Synapse Spark   │────▶│  Azure Data Lake │       │
│   │  Hubs (Kafka)    │     │  Streaming       │     │  Storage Gen2    │       │
│   │                  │     │                  │     │  (Parquet)       │       │
│   │  • transactions  │     │  • Enrichment    │     │                  │       │
│   │  • fraud-alerts  │     │  • Fraud Rules   │     │  • /transactions │       │
│   └──────────────────┘     │  • Scoring       │     │  • /alerts       │       │
│                            └──────────────────┘     │  • /models       │       │
│                                    │                └────────┬─────────┘       │
│                                    │                         │                  │
│                                    ▼                         ▼                  │
│                            ┌──────────────────┐     ┌──────────────────┐       │
│                            │  Synapse MLlib   │     │  Synapse SQL     │       │
│                            │                  │     │  Serverless      │       │
│                            │  • Training      │     │                  │       │
│                            │  • Evaluation    │     │  • Dashboards    │       │
│                            │  • Prediction    │     │  • Analytics     │       │
│                            └──────────────────┘     └────────┬─────────┘       │
│                                                              │                  │
│   ┌──────────────────────────────────────────────────────────┘                  │
│   │                                                                             │
│   ▼                                                                             │
│   ┌──────────────────┐     ┌──────────────────┐                                │
│   │  Power BI        │     │  Azure Data      │                                │
│   │  Dashboard       │     │  Factory         │                                │
│   │                  │     │  (Orchestration) │                                │
│   └──────────────────┘     └──────────────────┘                                │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 📋 Structure du Projet

```
azure/
├── README.md                           # Ce fichier
├── GUIDE_AZURE_COMPLET.md             # Guide step-by-step
│
├── producer/
│   ├── transaction_producer.py         # Producteur Event Hubs
│   ├── requirements.txt                # Dépendances Python
│   └── .env.example                    # Template configuration
│
├── synapse/
│   ├── notebooks/
│   │   ├── 01_fraud_detection_streaming.ipynb    # Spark Streaming
│   │   └── 02_ml_model_training.ipynb            # ML Pipeline
│   │
│   └── sql/
│       ├── 01_create_external_tables.sql         # Tables externes
│       └── 02_analytics_queries.sql              # Requêtes analytiques
│
└── data-factory/
    └── pipeline_guide.md               # Guide Data Factory
```

---

## 🚀 Quick Start

### Prérequis

1. **Compte Azure** avec crédits (Azure for Students: $100)
2. **Python 3.8+** pour le producer
3. **Azure CLI** (optionnel)

### Étape 1: Setup Azure (30 min)

Suivre le guide: `GUIDE_AZURE_COMPLET.md`

1. Créer Resource Group
2. Créer Storage Account (Data Lake Gen2)
3. Créer Event Hubs Namespace
4. Créer Synapse Workspace

### Étape 2: Lancer le Producer (5 min)

```bash
cd azure/producer

# Installer dépendances
pip install -r requirements.txt

# Configurer
cp .env.example .env
# Éditer .env avec vos credentials

# Lancer
python transaction_producer.py --count 10000 --fraud-rate 0.03
```

### Étape 3: Exécuter le Streaming (10 min)

1. Ouvrir Synapse Studio
2. Importer `01_fraud_detection_streaming.ipynb`
3. Modifier la configuration (storage account, etc.)
4. Exécuter toutes les cellules

### Étape 4: Analyser avec SQL

1. Dans Synapse Studio → Develop → SQL scripts
2. Exécuter `01_create_external_tables.sql`
3. Exécuter `02_analytics_queries.sql`

### Étape 5: Entraîner le ML

1. Importer `02_ml_model_training.ipynb`
2. Exécuter après avoir accumulé des données

---

## 🔧 Services Azure Utilisés

| Service | Rôle | Équivalent Open Source |
|---------|------|----------------------|
| Event Hubs | Message streaming | Apache Kafka |
| Synapse Spark | Processing distribué | Apache Spark |
| Data Lake Gen2 | Stockage | HDFS |
| Synapse SQL | Requêtes analytiques | Apache Impala |
| Data Factory | Orchestration | Apache Airflow |
| Power BI | Visualisation | Grafana/Superset |

---

## 💰 Estimation des Coûts

| Service | Coût/mois | Usage projet |
|---------|-----------|--------------|
| Event Hubs Basic | ~$11 | 1 TU, 2 topics |
| Synapse Spark (Small) | ~$0.60/h | ~30h = $18 |
| Data Lake Storage | ~$0.02/GB | 10 GB = $0.20 |
| Synapse SQL Serverless | ~$5/TB | ~10 GB = $0.05 |
| **TOTAL** | | **~$30-40** |

Avec $191 de crédits, vous pouvez faire le projet **4-5 fois**.

---

## 📊 Ce que vous pouvez dire en entretien

> "J'ai développé un système de détection de fraude en temps réel sur **Azure** avec:
>
> - **Azure Event Hubs** pour l'ingestion streaming (API Kafka-compatible)
> - **Synapse Spark Structured Streaming** pour le traitement temps réel
> - **Azure Data Lake Gen2** pour le stockage distribué en Parquet partitionné
> - **Synapse SQL Serverless** pour les requêtes analytiques massives
> - **Spark MLlib** pour un modèle Random Forest avec AUC > 0.95
> - **Azure Data Factory** pour l'orchestration des pipelines
>
> Le système traite des transactions en micro-batches de 30 secondes, applique des règles métier de scoring, et génère des alertes pour les transactions à haut risque.
>
> J'ai choisi Azure car il offre une intégration native entre les services, et l'architecture est facilement scalable pour des millions de transactions."

---

## 📁 Livrables pour le projet académique

1. **Code source** - Ce dossier `azure/`
2. **Screenshots** - Des différentes étapes sur Azure Portal
3. **Rapport** - Architecture, choix technologiques, résultats
4. **Présentation** - PowerPoint avec démo

---

## 🎓 Concepts démontrés

### Big Data
- ✅ Traitement distribué (Spark)
- ✅ Stockage distribué (Data Lake)
- ✅ Streaming temps réel

### Data Engineering
- ✅ ETL Pipeline
- ✅ Data Quality
- ✅ Partitioning

### Cloud Architecture
- ✅ Services managés
- ✅ Scalabilité
- ✅ Monitoring

### Machine Learning
- ✅ Feature Engineering
- ✅ Model Training
- ✅ Model Evaluation

---

## 🔗 Ressources

- [Azure Event Hubs Documentation](https://docs.microsoft.com/azure/event-hubs/)
- [Azure Synapse Analytics](https://docs.microsoft.com/azure/synapse-analytics/)
- [Spark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Azure for Students](https://azure.microsoft.com/free/students/)
