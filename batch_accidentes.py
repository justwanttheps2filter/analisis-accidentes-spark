
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, year, month, trim, upper
)
import matplotlib.pyplot as plt

# 1. Crear la sesión de Spark
spark = SparkSession.builder \
    .appName("AnalisisBatchAccidentes") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Cargar el archivo CSV
archivo = "accidentes_calarca.csv"

df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(archivo)

print("\n=== ESTRUCTURA DEL DATASET ===")
df.printSchema()

print("\n=== PRIMEROS REGISTROS ===")
df.show(10, truncate=False)

print("\n=== TOTAL DE REGISTROS ORIGINALES ===")
print(df.count())

# 3. Limpiar los datos
df_limpio = df

# Eliminar espacios de los nombres de columnas
for nombre in df_limpio.columns:
    nuevo_nombre = nombre.strip()
    if nombre != nuevo_nombre:
        df_limpio = df_limpio.withColumnRenamed(
            nombre, nuevo_nombre
        )

# Eliminar filas completamente duplicadas
df_limpio = df_limpio.dropDuplicates()

# Eliminar registros sin fecha o gravedad
df_limpio = df_limpio.filter(
    col("FECHA").isNotNull() &
    col("GRAVEDAD").isNotNull()
)

# Limpiar y estandarizar la gravedad
df_limpio = df_limpio.withColumn(
    "GRAVEDAD",
    upper(trim(col("GRAVEDAD").cast("string")))
)

print("\n=== TOTAL DE REGISTROS DESPUÉS DE LA LIMPIEZA ===")
print(df_limpio.count())

print("\n=== VALORES NULOS POR COLUMNA ===")
for nombre in df_limpio.columns:
    total_nulos = df_limpio.filter(
        col(nombre).isNull()
    ).count()
    print(f"{nombre}: {total_nulos}")

# 4. Análisis de accidentes por gravedad
por_gravedad = df_limpio.groupBy("GRAVEDAD") \
    .agg(count("*").alias("TOTAL")) \
    .orderBy(col("TOTAL").desc())

print("\n=== ACCIDENTES POR GRAVEDAD ===")
por_gravedad.show(truncate=False)

# 5. Análisis por año
por_anio = df_limpio.groupBy("AÑO") \
    .agg(count("*").alias("TOTAL")) \
    .orderBy("AÑO")

print("\n=== ACCIDENTES POR AÑO ===")
por_anio.show(truncate=False)

# 6. Análisis por mes
por_mes = df_limpio.groupBy("MES") \
    .agg(count("*").alias("TOTAL")) \
    .orderBy("MES")

print("\n=== ACCIDENTES POR MES ===")
por_mes.show(truncate=False)

# 7. Convertir resultados a Pandas para graficar
gravedad_pd = por_gravedad.toPandas()
anio_pd = por_anio.toPandas()
mes_pd = por_mes.toPandas()

# Gráfica 1: accidentes por gravedad
plt.figure(figsize=(9, 5))
plt.bar(
    gravedad_pd["GRAVEDAD"].astype(str),
    gravedad_pd["TOTAL"]
)
plt.title("Accidentes de tránsito por gravedad")
plt.xlabel("Gravedad")
plt.ylabel("Número de accidentes")
plt.xticks(rotation=25, ha="right")
plt.tight_layout()
plt.savefig("accidentes_por_gravedad.png", dpi=150)
plt.close()

# Gráfica 2: accidentes por año
plt.figure(figsize=(9, 5))
plt.bar(
    anio_pd["AÑO"].astype(str),
    anio_pd["TOTAL"]
)
plt.title("Accidentes de tránsito por año")
plt.xlabel("Año")
plt.ylabel("Número de accidentes")
plt.tight_layout()
plt.savefig("accidentes_por_anio.png", dpi=150)
plt.close()

# Gráfica 3: accidentes por mes
plt.figure(figsize=(9, 5))
plt.bar(
    mes_pd["MES"].astype(str),
    mes_pd["TOTAL"]
)
plt.title("Distribución de accidentes por mes")
plt.xlabel("Mes")
plt.ylabel("Número de accidentes")
plt.tight_layout()
plt.savefig("accidentes_por_mes.png", dpi=150)
plt.close()

# 8. Guardar el dataset limpio
df_limpio.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("resultado_accidentes_limpios")

print("\nProceso batch finalizado.")
print("Gráficas guardadas en el directorio actual.")
print("Datos limpios guardados en resultado_accidentes_limpios/")

spark.stop()
