# Guide de Démarrage Rapide

## Configuration de l'Environnement Local

---

## Prérequis Système

| Outil | Version | Obligatoire | Téléchargement |
|-------|---------|-------------|----------------|
| **Java JDK** | 11 ou 17 | ✅ Oui | [Adoptium](https://adoptium.net/) |
| **Docker Desktop** | 24+ | ✅ Oui | [Docker](https://www.docker.com/products/docker-desktop/) |
| **SBT** | 1.9+ | ✅ Oui | [SBT](https://www.scala-sbt.org/download.html) |
| **RAM** | 8 GB min | ✅ Oui | 16 GB recommandé |
| **Espace disque** | 10 GB | ✅ Oui | Pour images Docker |

---

## Installation Étape par Étape

### Étape 1: Installer Java 11

#### Windows
```powershell
# Télécharger depuis https://adoptium.net/
# Ou avec Chocolatey:
choco install temurin11
```

#### MacOS
```bash
brew install openjdk@11
echo 'export PATH="/opt/homebrew/opt/openjdk@11/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc
```

#### Linux (Ubuntu/Debian)
```bash
sudo apt update
sudo apt install openjdk-11-jdk
```

#### Vérifier l'installation
```bash
java -version
# Devrait afficher: openjdk version "11.x.x"
```

---

### Étape 2: Installer Docker Desktop

#### Windows
1. Télécharger [Docker Desktop](https://www.docker.com/products/docker-desktop/)
2. Exécuter l'installateur
3. **Important**: Activer WSL2 si demandé
4. Redémarrer l'ordinateur

#### MacOS
```bash
brew install --cask docker
# Ou télécharger depuis https://www.docker.com/products/docker-desktop/
```

#### Linux (Ubuntu/Debian)
```bash
# Installation automatique
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh

# Ajouter l'utilisateur au groupe docker
sudo usermod -aG docker $USER

# Redémarrer la session
newgrp docker
```

#### Vérifier l'installation
```bash
docker --version
docker run hello-world
```

---

### Étape 3: Installer SBT (Scala Build Tool)

#### Windows
```powershell
# Avec Chocolatey
choco install sbt

# Ou télécharger depuis https://www.scala-sbt.org/download.html
```

#### MacOS
```bash
brew install sbt
```

#### Linux (Ubuntu/Debian)
```bash
echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | sudo tee /etc/apt/sources.list.d/sbt.list
curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | sudo apt-key add
sudo apt update
sudo apt install sbt
```

#### Vérifier l'installation
```bash
sbt --version
```

---

## Exécution du Projet

### Option A: Mode Léger (Recommandé pour débuter)

Ce mode démarre uniquement Kafka pour tester le streaming.

```bash
# 1. Cloner ou aller dans le projet
cd Real_Time_Fraud_Detection

# 2. Démarrer Kafka + Zookeeper
cd docker
docker-compose -f docker-compose.light.yml up -d

# 3. Vérifier que ça fonctionne
docker ps
# Devrait afficher: zookeeper, kafka, kafka-ui

# 4. Ouvrir Kafka UI
# http://localhost:8080
```

### Option B: Mode Complet (Tout l'écosystème)

Ce mode démarre tout: Kafka, Spark, HDFS, Airflow, Prometheus, Grafana.

```bash
# 1. Aller dans le projet
cd Real_Time_Fraud_Detection

# 2. Démarrer toute l'infrastructure
cd docker
docker-compose up -d

# 3. Attendre 2-3 minutes que tout démarre
docker ps

# 4. Accéder aux interfaces
# - Kafka UI:     http://localhost:8080
# - Spark Master: http://localhost:8081
# - HDFS:         http://localhost:9870
# - Airflow:      http://localhost:8090 (admin/admin)
# - Grafana:      http://localhost:3000 (admin/admin)
```

---

## Compiler et Exécuter le Code Scala

### Compiler le projet

```bash
cd Real_Time_Fraud_Detection

# Première compilation (télécharge les dépendances - peut prendre 5-10 min)
sbt compile

# Créer le JAR exécutable
sbt assembly
# Crée: target/scala-2.12/fraud-detection-assembly.jar
```

### Exécuter les exemples

```bash
# Exemple 1: RDD Analysis (mode local)
sbt "runMain fraud.batch.RDDFraudAnalysis"

# Exemple 2: DataFrame Analysis
sbt "runMain fraud.batch.DataFrameFraudAnalysis"

# Exemple 3: Spark SQL
sbt "runMain fraud.batch.SparkSQLFraudAnalysis"

# Exemple 4: ML Model Training
sbt "runMain fraud.ml.FraudDetectionModel"
```

---

## Tester le Pipeline Complet

### 1. Démarrer l'infrastructure

```bash
cd docker
docker-compose -f docker-compose.light.yml up -d
```

### 2. Créer les topics Kafka

```bash
# Créer le topic transactions
docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --topic transactions --partitions 6 --replication-factor 1

# Créer le topic alerts
docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --topic fraud-alerts --partitions 3 --replication-factor 1

# Vérifier
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

### 3. Produire des transactions de test

```bash
# Terminal 1: Compiler et lancer le producteur
cd Real_Time_Fraud_Detection
sbt "runMain fraud.kafka.TransactionProducer continuous 100 0.05"

# Cela envoie 100 transactions/seconde avec 5% de fraudes
```

### 4. Consommer et traiter avec Spark Streaming

```bash
# Terminal 2: Lancer le streaming (nécessite Spark installé localement)
# Ou utiliser sbt:
sbt "runMain fraud.streaming.FraudDetectionStreaming"
```

### 5. Visualiser dans Kafka UI

Ouvrir http://localhost:8080 et :
- Voir le topic `transactions` avec les messages entrants
- Voir le topic `fraud-alerts` avec les alertes générées

---

## Résolution des Problèmes Courants

### Problème: "Port already in use"

```bash
# Trouver le processus qui utilise le port
lsof -i :9092  # ou le port concerné

# Arrêter le processus
kill -9 <PID>

# Ou arrêter tous les containers Docker
docker stop $(docker ps -aq)
```

### Problème: Docker ne démarre pas

```bash
# Windows: Vérifier que WSL2 est activé
wsl --status

# Linux: Vérifier le service Docker
sudo systemctl status docker
sudo systemctl start docker
```

### Problème: SBT très lent

```bash
# Augmenter la mémoire pour SBT
export SBT_OPTS="-Xmx2G -XX:+UseG1GC"

# Ou créer le fichier .sbtopts
echo "-J-Xmx2G" > .sbtopts
```

### Problème: Kafka ne reçoit pas de messages

```bash
# Vérifier les logs Kafka
docker logs kafka

# Vérifier la connectivité
docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092
```

### Problème: OutOfMemoryError dans Spark

```bash
# Augmenter la mémoire du driver
spark-submit --driver-memory 4g --executor-memory 4g ...

# Ou dans sbt
sbt -J-Xmx4G "runMain ..."
```

---

## Ressources Utiles

| Ressource | URL |
|-----------|-----|
| Documentation Spark | https://spark.apache.org/docs/latest/ |
| Documentation Kafka | https://kafka.apache.org/documentation/ |
| Scala Documentation | https://docs.scala-lang.org/ |
| SBT Documentation | https://www.scala-sbt.org/1.x/docs/ |

---

## Checklist de Démarrage

- [ ] Java 11+ installé (`java -version`)
- [ ] Docker installé (`docker --version`)
- [ ] Docker fonctionne (`docker run hello-world`)
- [ ] SBT installé (`sbt --version`)
- [ ] Projet cloné (`cd Real_Time_Fraud_Detection`)
- [ ] Docker Compose lancé (`docker-compose up -d`)
- [ ] Kafka UI accessible (http://localhost:8080)
- [ ] Compilation réussie (`sbt compile`)
