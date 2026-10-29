# Procesamiento de Accidentes de Tránsito con Apache Spark y Kafka

## 1. Descripción del proyecto

Este proyecto implementa técnicas de procesamiento de datos mediante Apache Spark y Apache Kafka para analizar información relacionada con accidentes de tránsito en el municipio de Calarcá, Quindío.

Se desarrollan dos enfoques:

- **Procesamiento por lotes (Batch):** permite cargar, limpiar, transformar y analizar los datos históricos de accidentes de tránsito.
- **Procesamiento en tiempo real (Streaming):** utiliza Apache Kafka para transmitir eventos simulados de accidentes y Apache Spark Structured Streaming para procesarlos y calcular estadísticas por gravedad.

El proyecto se desarrolla como parte de la actividad académica de procesamiento de datos con Apache Spark.

## 2. Fuente de datos

Los datos históricos se obtienen del conjunto de datos público disponible en el portal de datos abiertos de Colombia:

[Accidentes de tránsito desde marzo de 2017 hasta diciembre de 2022](https://www.datos.gov.co/Transporte/ACCIDENTES-DE-TRANSITO-DESDE-MARZO-2017-A-DICIEMBR/wacd-xkg8)

El archivo CSV se utiliza para realizar el procesamiento por lotes. Para la demostración de streaming se generan eventos sintéticos, identificados mediante el campo `SIMULADO`, por lo que estos eventos no corresponden a accidentes reales.

## 3. Tecnologías utilizadas

- Python 3.
- Apache Spark 3.5.8.
- Apache Kafka 3.9.2.
- Spark Structured Streaming.
- PySpark.
- `kafka-python`.
- Matplotlib.
- VirtualBox y Linux para el entorno de ejecución.

## 4. Estructura del repositorio

```text
analisis-accidentes-spark/
├── README.md
├── batch_accidentes.py
├── kafka_producer.py
├── spark_streaming_consumer.py
└── accidentes_calarca.csv
```

## 5. Descripción de los archivos

### `batch_accidentes.py`

Realiza el procesamiento por lotes del archivo histórico. Incluye la exploración de la estructura de los datos, la eliminación de duplicados, la limpieza de campos y el análisis de accidentes por gravedad, año y mes. También genera visualizaciones y guarda los datos procesados.

### `kafka_producer.py`

Genera eventos sintéticos de accidentes de tránsito en formato JSON y los envía al topic `sensor_data` de Kafka a intervalos de tres segundos.

### `spark_streaming_consumer.py`

Consume los mensajes publicados en Kafka, interpreta su contenido JSON y utiliza Spark Structured Streaming para calcular el número de accidentes por gravedad en ventanas de un minuto.

### `accidentes_calarca.csv`

Contiene los datos históricos utilizados en el procesamiento por lotes.

## 6. Requisitos previos

Antes de ejecutar el proyecto, se requiere:

- Python 3.
- Apache Spark instalado y configurado.
- Apache Kafka instalado y configurado.
- La biblioteca `kafka-python`.
- La biblioteca `matplotlib`.
- El conector de Kafka para Spark Structured Streaming.

Instalación de las bibliotecas de Python:

```bash
pip install kafka-python matplotlib
```

Se recomienda ejecutar los comandos desde el directorio donde se encuentran los archivos del proyecto.

## 7. Ejecución del procesamiento por lotes

Verifica que el archivo `accidentes_calarca.csv` esté en el mismo directorio que `batch_accidentes.py`.

Ejecuta el programa mediante:

```bash
/opt/spark/bin/spark-submit batch_accidentes.py
```

El procesamiento genera las siguientes salidas:

- `accidentes_por_gravedad.png`
- `accidentes_por_anio.png`
- `accidentes_por_mes.png`
- `resultado_accidentes_limpios/`

La carpeta `resultado_accidentes_limpios/` contiene los datos procesados en formato CSV.

## 8. Ejecución del procesamiento en tiempo real

### Paso 1. Iniciar ZooKeeper

```bash
sudo /opt/Kafka/bin/zookeeper-server-start.sh /opt/Kafka/config/zookeeper.properties
```

Mantén abierta esta terminal.

### Paso 2. Iniciar Kafka

En una segunda terminal:

```bash
sudo /opt/Kafka/bin/kafka-server-start.sh /opt/Kafka/config/server.properties
```

Mantén abierta esta terminal también.

### Paso 3. Crear el topic

Si el topic `sensor_data` todavía no existe, ejecuta:

```bash
/opt/Kafka/bin/kafka-topics.sh \
  --create \
  --bootstrap-server localhost:9092 \
  --replication-factor 1 \
  --partitions 1 \
  --topic sensor_data
```

Si ya existe, no es necesario crearlo nuevamente.

### Paso 4. Iniciar el productor

En una tercera terminal, desde el directorio del proyecto:

```bash
python3 kafka_producer.py
```

El programa comenzará a enviar eventos sintéticos al topic `sensor_data`.

### Paso 5. Iniciar el consumidor de Spark

En una cuarta terminal:

```bash
/opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
  spark_streaming_consumer.py
```

El consumidor mostrará las estadísticas de accidentes agrupadas por gravedad y ventana de tiempo.

Para detener los programas, utiliza `Ctrl+C` en las terminales correspondientes.

## 9. Resultados esperados

Con el procesamiento por lotes se busca identificar patrones en los registros históricos, como la distribución de accidentes por gravedad y su variación temporal.

Con el procesamiento en tiempo real se demuestra la integración entre Kafka y Spark Structured Streaming para recibir eventos, interpretarlos y actualizar estadísticas a medida que llegan nuevos mensajes.

Los resultados dependen de los datos disponibles y de la correcta configuración del entorno de ejecución.

## 10. Consideraciones

- Los registros del CSV corresponden a datos históricos de la fuente indicada.
- Los eventos generados por el productor de Kafka son simulados y se identifican con `SIMULADO = true`.
- El procesamiento batch y el streaming son dos flujos diferentes que se complementan en la demostración de las capacidades de procesamiento de datos.
- Las versiones y rutas indicadas corresponden al entorno de desarrollo utilizado en este proyecto.

## 11. Autoría

Proyecto académico desarrollado para la actividad de procesamiento de datos con Apache Spark de la Universidad Nacional Abierta y a Distancia (UNAD), Colombia.
