#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Streaming Processing COVID-19 Colombia - Top 10 Departamentos
Estudiante: Alder Ortiz - UNAD Big Data 2026
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc
from pyspark.sql.types import StructType, StructField, StringType

# Inicializar Spark Session
spark = SparkSession.builder     .appName("Streaming_Covid_AlderOrtiz")     .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Esquema completo con nombres exactos del CSV
schema = StructType([
    StructField("fecha reporte web", StringType(), True),
    StructField("ID de caso", StringType(), True),
    StructField("Fecha de notificación", StringType(), True),
    StructField("Código DIVIPOLA departamento", StringType(), True),
    StructField("Nombre departamento", StringType(), True),
    StructField("Código DIVIPOLA municipio", StringType(), True),
    StructField("Nombre municipio", StringType(), True),
    StructField("Edad", StringType(), True),
    StructField("Unidad de medida de edad", StringType(), True),
    StructField("Sexo", StringType(), True),
    StructField("Tipo de contagio", StringType(), True),
    StructField("Ubicación del caso", StringType(), True),
    StructField("Estado", StringType(), True),
    StructField("Código ISO del país", StringType(), True),
    StructField("Nombre del país", StringType(), True),
    StructField("Recuperado", StringType(), True),
    StructField("Fecha de inicio de síntomas", StringType(), True),
    StructField("Fecha de muerte", StringType(), True),
    StructField("Fecha de diagnóstico", StringType(), True),
    StructField("Fecha de recuperación", StringType(), True),
    StructField("Tipo de recuperación", StringType(), True),
    StructField("Pertenencia étnica", StringType(), True),
    StructField("Nombre del grupo étnico", StringType(), True)
])

# Lectura del stream desde HDFS
df_stream = spark.readStream     .option("header", True)     .option("encoding", "ISO-8859-1")     .option("maxFilesPerTrigger", "1")     .schema(schema)     .csv("hdfs://localhost:9000/input/")

# Limpieza de datos
df_clean = df_stream.select(
    col("Nombre departamento").alias("Departamento"),
    col("Edad").cast("int").alias("Edad"),
    col("Sexo"),
    col("Estado")
).filter(col("Nombre departamento").isNotNull())

# Procesamiento: Top 10 Departamentos
query = df_clean.groupBy("Departamento")     .count()     .orderBy(desc("count"))     .limit(10)     .writeStream     .outputMode("complete")     .format("console")     .option("truncate", False)     .trigger(processingTime="30 seconds")     .start()

# Mensajes de estado
print("\n" + "="*70)
print("🚀 STREAMING COVID-19 COLOMBIA - ACTIVO")
print("="*70)
print("🌐 Spark UI: http://192.168.1.45:4040")
print("📑 Pestaña: 'Structured Streaming'")
print("🛑 Detener: CTRL+C en esta terminal")
print("="*70 + "\n")

# Mantener ejecución
query.awaitTermination()
