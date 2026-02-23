# Azure Data Factory - Pipeline Orchestration

## Équivalent Airflow dans Azure

Data Factory est le service d'orchestration d'Azure, similaire à Apache Airflow.

## Architecture du Pipeline

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    DATA FACTORY PIPELINE                                │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│   ┌─────────────┐     ┌─────────────┐     ┌─────────────┐              │
│   │  Trigger    │────▶│   Validate  │────▶│   Start     │              │
│   │  (Schedule) │     │   Sources   │     │   Spark     │              │
│   └─────────────┘     └─────────────┘     └─────────────┘              │
│                                                  │                      │
│                                                  ▼                      │
│   ┌─────────────┐     ┌─────────────┐     ┌─────────────┐              │
│   │   Send      │◀────│  Refresh    │◀────│   Wait      │              │
│   │   Alerts    │     │   Tables    │     │   Complete  │              │
│   └─────────────┘     └─────────────┘     └─────────────┘              │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Étape 1: Créer Data Factory

### Via Azure Portal

1. **Azure Portal** → **Create a resource** → **Data Factory**
2. **Create**

```
Configuration:
├── Resource group:    rg-fraud-detection
├── Name:              adf-fraud-detection
├── Region:            France Central
└── Version:           V2
```

3. **Review + create** → **Create**

---

## Étape 2: Ouvrir Data Factory Studio

1. Aller dans la ressource créée
2. Cliquer **Launch Studio** ou aller sur https://adf.azure.com

---

## Étape 3: Créer les Linked Services

### 3.1 Linked Service - Azure Data Lake

1. **Manage** → **Linked services** → **+ New**
2. Chercher **Azure Data Lake Storage Gen2**

```
Name:               ls_datalake
Authentication:     Managed Identity
URL:                https://stfrauddetectionxxx.dfs.core.windows.net
```

3. **Test connection** → **Create**

### 3.2 Linked Service - Synapse

1. **+ New** → Chercher **Azure Synapse Analytics**

```
Name:               ls_synapse
Account selection:  From Azure subscription
Server name:        syn-fraud-detection.sql.azuresynapse.net
Database:           FraudDetectionDB
Authentication:     Managed Identity
```

---

## Étape 4: Créer le Pipeline Principal

### 4.1 Créer le Pipeline

1. **Author** → **+** → **Pipeline** → **Pipeline**
2. Nommer: `pl_fraud_detection_daily`

### 4.2 Ajouter les activités

Glisser-déposer depuis le panneau Activities:

```
1. Validation (GetMetadata)
   ├── Name: Validate_Source_Data
   └── Dataset: Transactions folder in Data Lake

2. Notebook (Synapse Notebook)
   ├── Name: Run_Streaming_Batch
   ├── Linked Service: ls_synapse
   └── Notebook: 01_fraud_detection_streaming

3. Stored Procedure (SQL)
   ├── Name: Refresh_SQL_Tables
   ├── Linked Service: ls_synapse
   └── Procedure: sp_RefreshViews

4. Web Activity
   ├── Name: Send_Alert_Notification
   ├── URL: [Your Logic App/Webhook URL]
   └── Method: POST

5. Set Variable
   ├── Name: Set_Completion_Status
   └── Variable: pipeline_status = "SUCCESS"
```

### 4.3 Connecter les activités

```
Validate_Source_Data
    │
    ▼ (On Success)
Run_Streaming_Batch
    │
    ▼ (On Success)
Refresh_SQL_Tables
    │
    ▼ (On Success)
Send_Alert_Notification
    │
    ▼ (On Success)
Set_Completion_Status
```

---

## Étape 5: Créer le Trigger (Planification)

### 5.1 Trigger Schedule

1. **Add trigger** → **New/Edit**
2. **+ New**

```
Name:           tr_daily_fraud_pipeline
Type:           Schedule
Start date:     [Date actuelle]
Recurrence:     Every 1 Day
Time:           02:00 AM
Time zone:      (UTC+01:00) Paris
```

### 5.2 Trigger Event-based (Optionnel)

Pour déclencher quand de nouveaux fichiers arrivent:

```
Name:           tr_new_files
Type:           Storage events
Storage:        stfrauddetectionxxx
Container:      raw-transactions
Events:         Blob created
```

---

## Étape 6: Pipeline Alternatif - Batch Processing

Créer un second pipeline pour le batch processing:

```yaml
Pipeline: pl_batch_processing
├── Activity 1: Check for New Data
│   └── Type: GetMetadata
│   └── Check: lastModified > last_run
│
├── Activity 2: Run ML Model Training
│   └── Type: Synapse Notebook
│   └── Notebook: 02_ml_model_training
│
├── Activity 3: Evaluate Model
│   └── Type: Synapse Notebook
│   └── Notebook: 03_model_evaluation
│
├── Activity 4: Deploy Model (if better)
│   └── Type: Web Activity
│   └── URL: ML Deployment endpoint
│
└── Activity 5: Update Metrics
    └── Type: Stored Procedure
    └── Proc: sp_UpdateModelMetrics
```

---

## Étape 7: Monitoring

### Dans Data Factory Studio

1. **Monitor** → **Pipeline runs**
2. Voir:
   - Status (Success/Failed/In Progress)
   - Duration
   - Activity runs detail

### Alertes

1. **Manage** → **Alerts & metrics**
2. Configurer alertes pour:
   - Pipeline failures
   - Long running pipelines
   - Data quality issues

---

## Pipeline JSON Export

Pour versionner dans Git, exporter le pipeline:

```json
{
    "name": "pl_fraud_detection_daily",
    "properties": {
        "activities": [
            {
                "name": "Validate_Source_Data",
                "type": "GetMetadata",
                "dependsOn": [],
                "policy": {
                    "timeout": "0.00:05:00",
                    "retry": 2
                },
                "typeProperties": {
                    "dataset": {
                        "referenceName": "ds_transactions",
                        "type": "DatasetReference"
                    },
                    "fieldList": ["exists", "itemName", "lastModified"]
                }
            },
            {
                "name": "Run_Streaming_Batch",
                "type": "SynapseNotebook",
                "dependsOn": [
                    {
                        "activity": "Validate_Source_Data",
                        "dependencyConditions": ["Succeeded"]
                    }
                ],
                "policy": {
                    "timeout": "0.01:00:00",
                    "retry": 1
                },
                "typeProperties": {
                    "notebook": {
                        "referenceName": "01_fraud_detection_streaming",
                        "type": "NotebookReference"
                    },
                    "sparkPool": {
                        "referenceName": "sparkpool",
                        "type": "BigDataPoolReference"
                    }
                }
            },
            {
                "name": "Refresh_SQL_Tables",
                "type": "SqlServerStoredProcedure",
                "dependsOn": [
                    {
                        "activity": "Run_Streaming_Batch",
                        "dependencyConditions": ["Succeeded"]
                    }
                ],
                "typeProperties": {
                    "storedProcedureName": "sp_RefreshViews"
                },
                "linkedServiceName": {
                    "referenceName": "ls_synapse",
                    "type": "LinkedServiceReference"
                }
            }
        ],
        "annotations": ["fraud-detection", "daily-pipeline"]
    }
}
```

---

## Comparaison avec Airflow

| Concept Airflow | Équivalent Data Factory |
|-----------------|------------------------|
| DAG | Pipeline |
| Task | Activity |
| Operator | Activity Type |
| Schedule | Trigger |
| XCom | Variables / Parameters |
| Connections | Linked Services |
| Sensors | GetMetadata / Until Loop |

---

## Prochaines étapes

1. Créer les datasets référencés
2. Configurer les paramètres du pipeline
3. Tester manuellement avec **Debug**
4. Publier et activer le trigger
