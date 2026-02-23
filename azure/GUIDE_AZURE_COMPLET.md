# 🚀 Guide Complet - Détection de Fraude sur Azure

## Architecture Production

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         AZURE FRAUD DETECTION PIPELINE                      │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   ┌─────────────┐     ┌─────────────┐     ┌─────────────────┐              │
│   │   Event     │────▶│   Synapse   │────▶│   Data Lake     │              │
│   │   Hubs      │     │   Spark     │     │   Storage Gen2  │              │
│   │ (Kafka API) │     │  Streaming  │     │   (Parquet)     │              │
│   └─────────────┘     └─────────────┘     └─────────────────┘              │
│         ▲                   │                     │                         │
│         │                   │                     ▼                         │
│   ┌─────────────┐           │             ┌─────────────────┐              │
│   │  Producer   │           │             │   Synapse       │              │
│   │  (Python)   │           │             │   SQL Pool      │              │
│   └─────────────┘           │             │  (Serverless)   │              │
│                             │             └─────────────────┘              │
│                             ▼                     │                         │
│                     ┌─────────────┐               ▼                         │
│                     │   MLlib     │       ┌─────────────────┐              │
│                     │   Model     │       │   Power BI      │              │
│                     └─────────────┘       │   Dashboard     │              │
│                                           └─────────────────┘              │
│   ┌─────────────────────────────────────────────────────────┐              │
│   │                    Data Factory                          │              │
│   │              (Orchestration - comme Airflow)             │              │
│   └─────────────────────────────────────────────────────────┘              │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

# 📋 ÉTAPE 1: Créer le Resource Group

Un Resource Group contient toutes les ressources Azure du projet.

## 1.1 Via Azure Portal

1. Aller sur https://portal.azure.com
2. Cliquer sur **"Create a resource"**
3. Chercher **"Resource group"**
4. Cliquer **Create**

**Configuration:**
```
Subscription:       Azure for Students
Resource group:     rg-fraud-detection
Region:             France Central (ou la plus proche)
```

5. Cliquer **Review + create** → **Create**

## 1.2 Via Azure CLI (Alternative)

```bash
# Installer Azure CLI: https://docs.microsoft.com/cli/azure/install-azure-cli

# Se connecter
az login

# Créer le resource group
az group create \
    --name rg-fraud-detection \
    --location francecentral
```

---

# 📋 ÉTAPE 2: Créer Azure Data Lake Storage Gen2

Le stockage pour nos fichiers Parquet (équivalent HDFS).

## 2.1 Créer le Storage Account

1. Azure Portal → **Create a resource** → **Storage account**
2. Cliquer **Create**

**Configuration:**
```
Basics:
├── Subscription:         Azure for Students
├── Resource group:       rg-fraud-detection
├── Storage account name: stfrauddetection[VOTRENOM]  (doit être unique)
├── Region:               France Central
├── Performance:          Standard
└── Redundancy:           LRS (Locally-redundant) - moins cher

Advanced:
└── Enable hierarchical namespace: ✅ COCHER (IMPORTANT pour Data Lake!)
```

3. Cliquer **Review + create** → **Create**

## 2.2 Créer les Containers (Dossiers)

1. Aller dans le Storage Account créé
2. Menu gauche → **Containers**
3. Créer les containers suivants:

```
+ Container: raw-transactions      (pour données brutes)
+ Container: processed-data        (pour données traitées)
+ Container: fraud-alerts          (pour les alertes)
+ Container: ml-models             (pour les modèles ML)
```

## 2.3 Récupérer les clés d'accès

1. Storage Account → **Access keys** (menu gauche)
2. Cliquer **Show** sur key1
3. **COPIER et SAUVEGARDER:**
   - Storage account name
   - Key1 (connection string)

```
⚠️ GARDER CES INFOS - on en aura besoin plus tard!

STORAGE_ACCOUNT_NAME = "stfrauddetection..."
STORAGE_ACCOUNT_KEY  = "xxxxxxxxxxxxxx..."
```

---

# 📋 ÉTAPE 3: Créer Azure Event Hubs (Kafka)

Event Hubs est le service de streaming compatible avec l'API Kafka.

## 3.1 Créer le Namespace Event Hubs

1. Azure Portal → **Create a resource** → **Event Hubs**
2. Cliquer **Create**

**Configuration:**
```
Basics:
├── Subscription:         Azure for Students
├── Resource group:       rg-fraud-detection
├── Namespace name:       evh-fraud-detection-[VOTRENOM]
├── Location:             France Central
├── Pricing tier:         Basic ($0.015/hour ≈ $11/mois)
└── Throughput Units:     1
```

3. Cliquer **Review + create** → **Create**

## 3.2 Créer les Event Hubs (Topics Kafka)

1. Aller dans le Namespace créé
2. Menu gauche → **Event Hubs**
3. Cliquer **+ Event Hub**

**Créer 2 Event Hubs:**

```
Event Hub 1:
├── Name:             transactions
├── Partition Count:  2 (suffisant pour le projet)
└── Message Retention: 1 day

Event Hub 2:
├── Name:             fraud-alerts
├── Partition Count:  2
└── Message Retention: 1 day
```

## 3.3 Créer une Shared Access Policy

1. Namespace → **Shared access policies** (menu gauche)
2. Cliquer **+ Add**

```
Policy name:    fraud-producer-consumer
Manage:         ✅
Send:           ✅
Listen:         ✅
```

3. Cliquer sur la policy créée
4. **COPIER et SAUVEGARDER:**
   - Connection string–primary key

```
⚠️ GARDER CETTE INFO!

EVENT_HUB_CONNECTION_STRING = "Endpoint=sb://evh-fraud-detection....servicebus.windows.net/;SharedAccessKeyName=...;SharedAccessKey=..."
```

---

# 📋 ÉTAPE 4: Créer Azure Synapse Analytics

Synapse combine Spark, SQL, et Data Integration (comme Airflow).

## 4.1 Créer le Synapse Workspace

1. Azure Portal → **Create a resource** → **Azure Synapse Analytics**
2. Cliquer **Create**

**Configuration:**
```
Basics:
├── Subscription:           Azure for Students
├── Resource group:         rg-fraud-detection
├── Managed resource group: mrg-fraud-detection (auto-créé)
├── Workspace name:         syn-fraud-detection
└── Region:                 France Central

Data Lake Storage:
├── Account name:           Sélectionner: stfrauddetection[VOTRENOM]
└── File system name:       synapse-data (sera créé)

Security:
├── SQL admin username:     sqladmin
└── SQL admin password:     [Mot de passe fort] ex: FraudDetect2024!
```

3. Cliquer **Review + create** → **Create** (⏱️ ~5-10 minutes)

## 4.2 Ouvrir Synapse Studio

1. Aller dans le Synapse Workspace créé
2. Cliquer **Open Synapse Studio** (ou aller sur https://web.azuresynapse.net)

## 4.3 Créer un Spark Pool

1. Dans Synapse Studio → **Manage** (icône toolbox à gauche)
2. **Apache Spark pools** → **+ New**

**Configuration:**
```
Basics:
├── Name:              sparkpool
├── Node size family:  Memory Optimized
├── Node size:         Small (4 vCores / 32 GB) - le moins cher
├── Autoscale:         ✅ Enabled
├── Min nodes:         3
└── Max nodes:         3

Additional settings:
├── Automatic pausing:  ✅ Enabled
├── Idle minutes:       15 (s'arrête si inactif = économies!)
└── Apache Spark:       3.3 (ou plus récent)
```

3. Cliquer **Review + create** → **Create** (⏱️ ~5 minutes)

---

# 📋 ÉTAPE 5: Configuration des Linked Services

Connecter Synapse à Event Hubs et Data Lake.

## 5.1 Linked Service pour Data Lake

1. Synapse Studio → **Manage** → **Linked services**
2. **+ New** → Chercher **Azure Data Lake Storage Gen2**

```
Name:                 ls_datalake
Authentication:       Account key
Storage account:      stfrauddetection[VOTRENOM]
```

3. **Test connection** → **Create**

## 5.2 Linked Service pour Event Hubs

1. **+ New** → Chercher **Azure Event Hubs**

```
Name:                      ls_eventhubs
Event Hub namespace:       evh-fraud-detection-[VOTRENOM]
Event Hub name:            transactions
Authentication:            Connection string
Connection string:         [Coller la connection string sauvegardée]
```

3. **Test connection** → **Create**

---

# 📋 ÉTAPE 6: Tester le Setup

## 6.1 Test rapide dans Synapse Studio

1. **Develop** → **+** → **Notebook**
2. Attacher au **sparkpool**
3. Exécuter:

```python
# Cell 1: Test Spark
print(f"Spark version: {spark.version}")
print("✅ Spark Pool fonctionne!")

# Cell 2: Test Data Lake
df = spark.createDataFrame([
    ("TX001", 100.0, "Paris"),
    ("TX002", 250.0, "London")
], ["id", "amount", "city"])

df.write.mode("overwrite").parquet(
    "abfss://processed-data@stfrauddetection[VOTRENOM].dfs.core.windows.net/test/"
)
print("✅ Data Lake fonctionne!")

# Cell 3: Lire
df_read = spark.read.parquet(
    "abfss://processed-data@stfrauddetection[VOTRENOM].dfs.core.windows.net/test/"
)
df_read.show()
```

---

# 💰 ESTIMATION COÛTS

| Service | Coût/heure | Usage estimé | Total |
|---------|------------|--------------|-------|
| Event Hubs Basic | $0.015/h | 100h | ~$1.50 |
| Synapse Spark (Small, 3 nodes) | $0.60/h | 30h | ~$18 |
| Data Lake Storage | $0.02/GB/mois | 5 GB | ~$0.10 |
| Synapse SQL Serverless | $5/TB scanné | 10 GB | ~$0.05 |
| **TOTAL ESTIMÉ** | | | **~$20-30** |

**Il te restera ~$160 de crédits après le projet!**

---

# ⏭️ PROCHAINES ÉTAPES

Une fois le setup terminé, on va:

1. ✅ Exécuter le Producer Python pour envoyer des transactions
2. ✅ Exécuter le notebook Spark Streaming
3. ✅ Analyser avec Synapse SQL
4. ✅ Entraîner le modèle ML
5. ✅ Créer le pipeline Data Factory

Passe à l'étape suivante: **`02_EVENT_HUBS_PRODUCER.md`**
