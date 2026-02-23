# 📋 Quick Reference Card - Azure Fraud Detection

## Ordre d'exécution

```
1. SETUP AZURE (Portal)
   └── Resource Group → Storage → Event Hubs → Synapse

2. PRODUCER (Local/Cloud Shell)
   └── python transaction_producer.py --count 10000

3. STREAMING (Synapse Studio)
   └── 01_fraud_detection_streaming.ipynb

4. SQL ANALYTICS (Synapse Studio)
   └── 01_create_external_tables.sql
   └── 02_analytics_queries.sql

5. MACHINE LEARNING (Synapse Studio)
   └── 02_ml_model_training.ipynb
```

---

## 🔑 Variables à remplacer

Dans tous les fichiers, remplacer:

| Variable | Exemple | Où la trouver |
|----------|---------|---------------|
| `stfrauddetectionxxx` | `stfrauddetection123` | Storage Account → Name |
| `evh-fraud-detection-xxx` | `evh-fraud-detection-john` | Event Hubs → Namespace |
| `EVENT_HUB_CONNECTION_STRING` | `Endpoint=sb://...` | Event Hubs → Shared access policies |
| `syn-fraud-detection` | Votre nom | Synapse Workspace |

---

## 🛠️ Commandes utiles

### Azure CLI
```bash
# Login
az login

# Lister les ressources
az resource list --resource-group rg-fraud-detection --output table

# Voir les coûts
az consumption usage list --output table
```

### Producer
```bash
# Installation
pip install azure-eventhub python-dotenv

# Lancer 10000 transactions
python transaction_producer.py --count 10000 --fraud-rate 0.03

# Lancer en continu (batch par batch)
python transaction_producer.py --count 100000 --batch-size 500 --delay 1
```

### Synapse Spark
```python
# Lire depuis Data Lake
df = spark.read.parquet("abfss://container@storage.dfs.core.windows.net/path/")

# Écrire en Parquet partitionné
df.write.partitionBy("col").parquet("path")

# Streaming
df.writeStream.format("parquet").start()
```

---

## 📊 Métriques attendues

| Métrique | Valeur cible |
|----------|-------------|
| Transactions/sec | 100-500 |
| Latence streaming | < 30 sec |
| AUC-ROC (ML) | > 0.90 |
| Precision | > 0.70 |
| Recall | > 0.60 |

---

## ⚠️ Points d'attention

1. **Coûts**: Arrêter le Spark Pool quand pas utilisé (auto-pause: 15 min)
2. **Event Hubs**: Retention 1 jour suffit pour le projet
3. **Storage**: LRS (pas GRS) pour économiser
4. **Streaming**: Trigger 30 sec pour démo, 1 min en prod

---

## 🔥 Pour impressionner en entretien

Mentionner ces termes techniques:

- **Micro-batch processing** vs continuous streaming
- **Partition pruning** dans les requêtes SQL
- **Feature engineering** pour le ML
- **Checkpointing** pour la fault tolerance
- **Exactly-once semantics**
- **Schema evolution** avec Parquet
- **Auto-scaling** Synapse Spark pools

---

## 📸 Screenshots à capturer

1. ✅ Architecture Resource Group (toutes les ressources)
2. ✅ Event Hubs monitoring (messages in/out)
3. ✅ Synapse Spark job running
4. ✅ Data Lake structure (partitions)
5. ✅ SQL query results
6. ✅ ML model metrics
7. ✅ Data Factory pipeline
