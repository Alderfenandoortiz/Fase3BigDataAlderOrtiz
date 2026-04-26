# 🚀 Tarea 3 - Procesamiento de Datos con Apache Spark

**Estudiante:** Alder Ortiz  
**Curso:** Big Data - Código: 202016911  
**Universidad:** UNAD - Escuela de Ciencias Básicas, Tecnología e Ingeniería  
**Fecha:** Abril 2026  

**Infraestructura:**
- IP Máquina Virtual: `192.168.1.45`  
- Usuario: `vboxuser`  
- Contraseña: `bigdata`  

---

## 📌 Descripción General

En este repositorio se presenta la implementación de un sistema de procesamiento de datos **Batch y Streaming**, desarrollado como parte de la Tarea 3 del curso de Big Data.

Tecnologías utilizadas:
- Apache Spark  
- Apache Kafka  
- Hadoop HDFS  

---

## 📋 Descripción de la Solución

### 🧱 Procesamiento Batch
Análisis histórico del dataset **Casos positivos de COVID-19 en Colombia** almacenado en HDFS.

### ⚡ Procesamiento en Tiempo Real
Simulación y consumo de datos mediante Kafka y procesamiento con Spark Streaming según el Anexo 3.

---

## 🗂️ Estructura del Proyecto

```text
📁 tarea_3_alder_ortiz/
├── tarea_3_alder_ortiz.py
├── kafka_productor.py
├── spark_streaming.py
├── streaming_covid_alder_ortiz.py
├── README.md
└── top10_covid.png
```

---

## 🚀 Guía de Ejecución

### 1. Requisitos Previos

```bash
start-dfs.sh
jps
hdfs dfs -ls /input/
```

---

### 2. Procesamiento Batch

```bash
spark-submit /home/vboxuser/tarea_3_alder_ortiz.py
```

**Resultado:**  
`/home/vboxuser/top10_covid.png`

**Spark UI:**  
http://192.168.1.45:4040  

---

### 3. Streaming con Kafka

#### Iniciar servicios

```bash
sudo /opt/Kafka/bin/zookeeper-server-start.sh /opt/Kafka/config/zookeeper.properties &
sudo /opt/Kafka/bin/kafka-server-start.sh /opt/Kafka/config/server.properties &
```

#### Crear tópico

```bash
/opt/Kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic sensor_data
```

#### Ejecutar productor

```bash
python3 /home/vboxuser/kafka_productor.py
```

#### Ejecutar consumidor

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 /home/vboxuser/spark_streaming.py
```

---

## ⚙️ Configuración Técnica

- Encoding: ISO-8859-1  
- Windowing:
```python
.groupBy(window(col("timestamp"), "1 minute"), "sensor_id")
```
- Checkpointing habilitado  

---

## 📊 Dataset Utilizado

- Fuente: Datos Abiertos Colombia  
- Formato: CSV  
- Almacenamiento: HDFS  

---

## 📚 Referencias

- Maldonado & Velásquez (2022)  
- Macías & Gómez (2015)  
- UNAD (2026)  

---

## ✅ Conclusión

El proyecto implementa correctamente una arquitectura de Big Data combinando procesamiento Batch y Streaming, cumpliendo con los lineamientos académicos y aplicando buenas prácticas en ingeniería de datos.
