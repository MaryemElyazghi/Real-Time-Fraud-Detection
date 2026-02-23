"""
Fraud Detection Pipeline - Airflow DAG
======================================

Ce DAG orchestre le pipeline complet de détection de fraude:
1. Ingestion des données depuis Kafka
2. Traitement Spark pour détection de fraude
3. Stockage des résultats dans HDFS
4. Rafraîchissement des tables Impala
5. Génération des rapports et alertes

Fréquence: Toutes les 5 minutes pour le traitement batch
Le streaming est géré séparément par Spark Structured Streaming
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.apache.hdfs.sensors.hdfs import HdfsSensor
from airflow.utils.trigger_rule import TriggerRule
import json
import logging

# Configuration par défaut
default_args = {
    'owner': 'fraud-detection-team',
    'depends_on_past': False,
    'email': ['alerts@fraud-detection.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
}

# Variables de configuration
SPARK_MASTER = "spark://spark-master:7077"
HDFS_NAMENODE = "hdfs://namenode:9000"
KAFKA_BROKERS = "kafka:29092"
JAR_PATH = "/opt/spark-apps/fraud-detection-assembly.jar"

# ==============================================================================
# FONCTIONS UTILITAIRES
# ==============================================================================

def check_data_quality(**context):
    """
    Vérifie la qualité des données traitées
    """
    logging.info("Checking data quality...")

    # Simuler la vérification de qualité
    # En production, cela interrogerait HDFS ou une base de données
    metrics = {
        'total_records': 100000,
        'null_rate': 0.001,
        'duplicate_rate': 0.0005,
        'schema_valid': True
    }

    # Push metrics to XCom
    context['ti'].xcom_push(key='data_quality_metrics', value=metrics)

    # Vérifier les seuils
    if metrics['null_rate'] > 0.05:
        raise ValueError(f"Null rate too high: {metrics['null_rate']}")
    if metrics['duplicate_rate'] > 0.01:
        raise ValueError(f"Duplicate rate too high: {metrics['duplicate_rate']}")

    logging.info(f"Data quality check passed: {metrics}")
    return True


def generate_fraud_report(**context):
    """
    Génère un rapport de fraude quotidien
    """
    execution_date = context['execution_date']

    report = {
        'date': execution_date.strftime('%Y-%m-%d'),
        'total_transactions': 5000000,
        'fraud_detected': 2500,
        'fraud_rate': 0.0005,
        'blocked_amount': 1250000.00,
        'top_fraud_categories': [
            {'category': 'GAMBLING', 'count': 500},
            {'category': 'CRYPTO', 'count': 400},
            {'category': 'WIRE_TRANSFER', 'count': 300}
        ],
        'model_performance': {
            'precision': 0.95,
            'recall': 0.88,
            'f1_score': 0.91
        }
    }

    logging.info(f"Generated fraud report: {json.dumps(report, indent=2)}")
    context['ti'].xcom_push(key='fraud_report', value=report)

    return report


def should_retrain_model(**context):
    """
    Décide si le modèle ML doit être réentraîné
    """
    # Pull les métriques du modèle
    ti = context['ti']
    report = ti.xcom_pull(task_ids='generate_report', key='fraud_report')

    if report:
        precision = report.get('model_performance', {}).get('precision', 1.0)
        recall = report.get('model_performance', {}).get('recall', 1.0)

        # Réentraîner si les performances dégradent
        if precision < 0.90 or recall < 0.85:
            logging.info("Model performance degraded, triggering retraining")
            return 'retrain_model'

    logging.info("Model performance acceptable, skipping retraining")
    return 'skip_retraining'


def send_alerts(**context):
    """
    Envoie des alertes pour les fraudes critiques
    """
    logging.info("Sending fraud alerts...")

    # En production, cela enverrait des emails, SMS, Slack, etc.
    alerts = [
        {'type': 'CRITICAL', 'count': 10, 'channel': 'sms'},
        {'type': 'HIGH', 'count': 50, 'channel': 'email'},
        {'type': 'MEDIUM', 'count': 200, 'channel': 'slack'}
    ]

    for alert in alerts:
        logging.info(f"Sent {alert['count']} {alert['type']} alerts via {alert['channel']}")

    return True


# ==============================================================================
# DAG PRINCIPAL - TRAITEMENT BATCH
# ==============================================================================

with DAG(
    dag_id='fraud_detection_batch_pipeline',
    default_args=default_args,
    description='Pipeline batch de détection de fraude',
    schedule_interval='*/15 * * * *',  # Toutes les 15 minutes
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['fraud-detection', 'batch', 'spark'],
    max_active_runs=1,
) as batch_dag:

    # Début du pipeline
    start = EmptyOperator(task_id='start')

    # Vérifier la disponibilité des données
    check_hdfs_data = BashOperator(
        task_id='check_hdfs_data',
        bash_command='''
            hdfs dfs -test -d /fraud-detection/raw/transactions/$(date +%Y/%m/%d) && echo "Data exists" || echo "No data"
        ''',
    )

    # Traitement Spark - Agrégation batch
    spark_batch_processing = SparkSubmitOperator(
        task_id='spark_batch_processing',
        application=JAR_PATH,
        name='fraud-detection-batch',
        conn_id='spark_default',
        conf={
            'spark.master': SPARK_MASTER,
            'spark.executor.memory': '2g',
            'spark.executor.cores': '2',
            'spark.driver.memory': '1g',
            'spark.sql.shuffle.partitions': '200',
        },
        application_args=[
            '--mode', 'batch',
            '--input', f'{HDFS_NAMENODE}/fraud-detection/raw/transactions',
            '--output', f'{HDFS_NAMENODE}/fraud-detection/processed/batch'
        ],
        java_class='fraud.batch.DataFrameFraudAnalysis',
    )

    # Vérification qualité des données
    data_quality_check = PythonOperator(
        task_id='data_quality_check',
        python_callable=check_data_quality,
    )

    # Rafraîchir les métadonnées Impala
    refresh_impala = BashOperator(
        task_id='refresh_impala_metadata',
        bash_command='''
            impala-shell -i impala:21000 -q "
                INVALIDATE METADATA fraud_detection.transactions;
                INVALIDATE METADATA fraud_detection.predictions;
                COMPUTE STATS fraud_detection.transactions;
                COMPUTE STATS fraud_detection.predictions;
            "
        ''',
    )

    # Générer le rapport
    generate_report = PythonOperator(
        task_id='generate_report',
        python_callable=generate_fraud_report,
    )

    # Décider du réentraînement
    check_model_performance = BranchPythonOperator(
        task_id='check_model_performance',
        python_callable=should_retrain_model,
    )

    # Réentraîner le modèle si nécessaire
    retrain_model = SparkSubmitOperator(
        task_id='retrain_model',
        application=JAR_PATH,
        name='fraud-model-training',
        conn_id='spark_default',
        conf={
            'spark.master': SPARK_MASTER,
            'spark.executor.memory': '4g',
            'spark.executor.cores': '4',
        },
        application_args=[
            '--mode', 'train',
            '--training-data', f'{HDFS_NAMENODE}/fraud-detection/training',
            '--model-output', f'{HDFS_NAMENODE}/fraud-detection/models/latest'
        ],
        java_class='fraud.ml.FraudDetectionModel',
    )

    # Skip le réentraînement
    skip_retraining = EmptyOperator(task_id='skip_retraining')

    # Rejoindre les branches
    join_branches = EmptyOperator(
        task_id='join_branches',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # Envoyer les alertes
    send_fraud_alerts = PythonOperator(
        task_id='send_fraud_alerts',
        python_callable=send_alerts,
    )

    # Fin du pipeline
    end = EmptyOperator(task_id='end')

    # Définition du flux
    start >> check_hdfs_data >> spark_batch_processing >> data_quality_check
    data_quality_check >> refresh_impala >> generate_report >> check_model_performance
    check_model_performance >> [retrain_model, skip_retraining]
    [retrain_model, skip_retraining] >> join_branches >> send_fraud_alerts >> end


# ==============================================================================
# DAG STREAMING - SURVEILLANCE
# ==============================================================================

with DAG(
    dag_id='fraud_detection_streaming_monitor',
    default_args=default_args,
    description='Surveillance du pipeline streaming',
    schedule_interval='*/5 * * * *',  # Toutes les 5 minutes
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['fraud-detection', 'streaming', 'monitoring'],
) as monitor_dag:

    start_monitor = EmptyOperator(task_id='start_monitor')

    # Vérifier que le job Spark Streaming tourne
    check_streaming_job = BashOperator(
        task_id='check_streaming_job',
        bash_command='''
            APPS=$(curl -s http://spark-master:8080/json/ | jq '.activeapps[] | select(.name | contains("FraudDetection"))')
            if [ -z "$APPS" ]; then
                echo "WARNING: No streaming job found!"
                exit 1
            else
                echo "Streaming job is running"
            fi
        ''',
    )

    # Vérifier le lag Kafka
    check_kafka_lag = BashOperator(
        task_id='check_kafka_lag',
        bash_command='''
            kafka-consumer-groups --bootstrap-server kafka:29092 \
                --group fraud-detection-group \
                --describe 2>/dev/null | grep -v "^$"
        ''',
    )

    # Vérifier les métriques de latence
    check_latency = BashOperator(
        task_id='check_latency',
        bash_command='''
            # Vérifier la latence moyenne des dernières 5 minutes
            LATENCY=$(curl -s http://prometheus:9090/api/v1/query \
                --data-urlencode 'query=avg(fraud_detection_processing_latency_seconds)' \
                | jq '.data.result[0].value[1]')

            echo "Average latency: ${LATENCY}s"

            # Alerter si latence > 100ms
            if (( $(echo "$LATENCY > 0.1" | bc -l) )); then
                echo "WARNING: High latency detected!"
            fi
        ''',
    )

    # Collecter les métriques pour Grafana
    collect_metrics = BashOperator(
        task_id='collect_metrics',
        bash_command='''
            # Métriques à collecter
            METRICS=$(cat <<EOF
            {
                "timestamp": "$(date -Iseconds)",
                "transactions_per_second": $(curl -s http://prometheus:9090/api/v1/query --data-urlencode 'query=rate(fraud_transactions_total[1m])' | jq '.data.result[0].value[1]'),
                "frauds_detected": $(curl -s http://prometheus:9090/api/v1/query --data-urlencode 'query=fraud_detected_total' | jq '.data.result[0].value[1]'),
                "avg_latency_ms": $(curl -s http://prometheus:9090/api/v1/query --data-urlencode 'query=avg(fraud_detection_latency_ms)' | jq '.data.result[0].value[1]')
            }
            EOF
            )
            echo "$METRICS"
        ''',
    )

    end_monitor = EmptyOperator(task_id='end_monitor')

    start_monitor >> [check_streaming_job, check_kafka_lag, check_latency]
    [check_streaming_job, check_kafka_lag, check_latency] >> collect_metrics >> end_monitor


# ==============================================================================
# DAG QUOTIDIEN - MAINTENANCE
# ==============================================================================

with DAG(
    dag_id='fraud_detection_daily_maintenance',
    default_args=default_args,
    description='Maintenance quotidienne du système',
    schedule_interval='0 2 * * *',  # Tous les jours à 2h du matin
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['fraud-detection', 'maintenance'],
) as maintenance_dag:

    start_maintenance = EmptyOperator(task_id='start_maintenance')

    # Archiver les anciennes données
    archive_old_data = BashOperator(
        task_id='archive_old_data',
        bash_command='''
            # Archiver les données de plus de 30 jours
            ARCHIVE_DATE=$(date -d '30 days ago' +%Y/%m/%d)
            hdfs dfs -mv /fraud-detection/raw/transactions/${ARCHIVE_DATE} \
                /fraud-detection/archive/transactions/${ARCHIVE_DATE} 2>/dev/null || true
        ''',
    )

    # Nettoyer les checkpoints anciens
    cleanup_checkpoints = BashOperator(
        task_id='cleanup_checkpoints',
        bash_command='''
            # Supprimer les checkpoints de plus de 7 jours
            find /tmp/spark-checkpoints -type d -mtime +7 -exec rm -rf {} \; 2>/dev/null || true
        ''',
    )

    # Optimiser les tables Impala
    optimize_impala = BashOperator(
        task_id='optimize_impala',
        bash_command='''
            impala-shell -i impala:21000 -q "
                -- Compacter les petits fichiers
                ALTER TABLE fraud_detection.transactions RECOVER PARTITIONS;

                -- Mettre à jour les statistiques
                COMPUTE INCREMENTAL STATS fraud_detection.transactions;
                COMPUTE INCREMENTAL STATS fraud_detection.predictions;
            "
        ''',
    )

    # Sauvegarder le modèle
    backup_model = BashOperator(
        task_id='backup_model',
        bash_command='''
            DATE=$(date +%Y%m%d)
            hdfs dfs -cp /fraud-detection/models/latest \
                /fraud-detection/models/backup/model_${DATE}
        ''',
    )

    # Rapport de santé système
    system_health_report = BashOperator(
        task_id='system_health_report',
        bash_command='''
            echo "=== System Health Report $(date) ==="
            echo ""
            echo "=== HDFS Usage ==="
            hdfs dfs -du -h /fraud-detection | head -20
            echo ""
            echo "=== Kafka Topics ==="
            kafka-topics --bootstrap-server kafka:29092 --list
            echo ""
            echo "=== Spark Applications ==="
            curl -s http://spark-master:8080/json/ | jq '.activeapps | length'
        ''',
    )

    end_maintenance = EmptyOperator(task_id='end_maintenance')

    start_maintenance >> [archive_old_data, cleanup_checkpoints]
    [archive_old_data, cleanup_checkpoints] >> optimize_impala >> backup_model
    backup_model >> system_health_report >> end_maintenance
