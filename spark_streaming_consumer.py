
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window, count
)
from pyspark.sql.types import (
    StructType, StructField, IntegerType,
    StringType, BooleanType
)

# Crear la sesión de Spark
spark = SparkSession.builder \
    .appName("AccidentesSparkStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Definir el esquema de los accidentes
schema = StructType([
    StructField("ORDEN", IntegerType(), True),
    StructField("IPAT", StringType(), True),
    StructField("FECHA", StringType(), True),
    StructField("AÑO", IntegerType(), True),
    StructField("MES", IntegerType(), True),
    StructField("DIA", StringType(), True),
    StructField("GRAVEDAD", StringType(), True),
    StructField("SIMULADO", BooleanType(), True)
])

# Leer eventos desde Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor_data") \
    .option("startingOffsets", "latest") \
    .load()

# Convertir los mensajes JSON en columnas
accidentes = df.select(
    from_json(
        col("value").cast("string"),
        schema
    ).alias("data")
).select("data.*")

# Convertir la fecha a timestamp
accidentes = accidentes.withColumn(
    "FECHA_TS",
    to_timestamp(col("FECHA"), "yyyy-MM-dd'T'HH:mm:ss")
)

# Agrupar accidentes por gravedad en ventanas de un minuto
estadisticas = accidentes \
    .withWatermark("FECHA_TS", "2 minutes") \
    .groupBy(
        window(col("FECHA_TS"), "1 minute"),
        col("GRAVEDAD")
    ) \
    .agg(count("*").alias("TOTAL_ACCIDENTES"))

# Mostrar resultados en la consola
consulta = estadisticas.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .start()

consulta.awaitTermination()
