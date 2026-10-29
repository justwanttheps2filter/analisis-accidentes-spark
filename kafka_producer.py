
import time
import json
import random
from datetime import datetime
from kafka import KafkaProducer


def generate_accident_data():
    dias = [
        "lunes", "martes", "miércoles", "jueves",
        "viernes", "sábado", "domingo"
    ]

    ahora = datetime.now()

    return {
        "ORDEN": random.randint(1, 100000),
        "IPAT": f"SIM-{random.randint(1, 999999):06d}",
        "FECHA": ahora.isoformat(timespec="seconds"),
        "AÑO": ahora.year,
        "MES": ahora.month,
        "DIA": dias[ahora.weekday()],
        "GRAVEDAD": random.choice([
            "SOLO DANOS",
            "CON HERIDOS",
            "CON MUERTOS"
        ]),
        "SIMULADO": True
    }


producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda x: json.dumps(x).encode("utf-8")
)

try:
    while True:
        accident_data = generate_accident_data()

        producer.send("sensor_data", value=accident_data)
        producer.flush()

        print(f"Accidente enviado: {accident_data}")
        time.sleep(3)

except KeyboardInterrupt:
    print("\nProductor detenido.")

finally:
    producer.close()
