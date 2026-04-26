#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Batch Processing COVID-19 Colombia - Top 10 Departamentos
Estudiante: Alder Ortiz - UNAD Big Data 2026
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import pandas as pd

# Inicializar Spark Session
spark = SparkSession.builder     .appName("AnalisisBatch_Covid_AlderOrtiz")     .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Ruta HDFS completa
PATH_HDFS = "hdfs://localhost:9000/input/Casos_positivos_de_COVID-19_en_Colombia._20260419.csv"
print(f"📁 Leyendo datos desde: {PATH_HDFS}")

# Cargar datos desde HDFS
df = spark.read     .option("header", True)     .option("encoding", "ISO-8859-1")     .csv(PATH_HDFS)

print("✅ Datos cargados exitosamente")
df.show(5)

# Limpieza de datos
df_clean = df.select(
    col("Nombre departamento").alias("Departamento"),
    col("Edad").cast("int").alias("Edad"),
    col("Sexo"),
    col("Estado")
).na.drop()

print("✅ Limpieza aplicada")
df_clean.show(5)

# Crear vista temporal para SQL
df_clean.createOrReplaceTempView("tabla_covid")

# Análisis con SQL: Top 10 Departamentos
resultado = spark.sql("""
    SELECT Departamento, COUNT(*) as Total
    FROM tabla_covid
    GROUP BY Departamento
    ORDER BY Total DESC
    LIMIT 10
""")

print("\n📊 Top 10 departamentos:")
resultado.show()

# Convertir a Pandas y generar gráfica
pdf = resultado.toPandas()
plt.figure(figsize=(10,6))
plt.bar(pdf['Departamento'], pdf['Total'])
plt.xticks(rotation=45, ha='right')
plt.title('Top 10 Departamentos - COVID Colombia')
plt.tight_layout()

# Guardar imagen de evidencia
ruta_imagen = "/home/vboxuser/top10_covid.png"
plt.savefig(ruta_imagen, dpi=300, bbox_inches='tight')
print(f"✅ Gráfica guardada en: {ruta_imagen}")

# Mensaje final - SIN spark.stop() para mantener UI activa
print("\n" + "="*70)
print("✅ Procesamiento batch completado")
print("🌐 Spark UI disponible en: http://192.168.1.45:4040")
print("🛑 Presiona ENTER para finalizar")
print("="*70)

input()  # Mantiene el script activo hasta que presiones ENTER
