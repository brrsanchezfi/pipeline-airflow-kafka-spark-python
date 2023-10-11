from confluent_kafka import Consumer, KafkaError

# Configuración del consumidor de Kafka
consumer_config = {
    'bootstrap.servers': 'localhost:9092',  # Cambia esto según la configuración de tu clúster Kafka
    'group.id': 'my_consumer_group',        # Cambia esto según tu grupo de consumidores
    'auto.offset.reset': 'earliest'         # Cambia esto según tus necesidades de lectura
}


consumer = Consumer(consumer_config)
consumer.subscribe(['airflow-spark'])

try:
    while True:
        msg = consumer.poll(1.0)  # Espera por mensajes durante 1 segundo

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print('Llegamos al final del tema')
            else:
                print(f'Error al recibir mensaje: {msg.error().str()}')
        else:
            # Procesa el mensaje JSON recibido
            json_data = msg.value().decode('utf-8')
            print(f'Mensaje JSON recibido: {json_data}')

except KeyboardInterrupt:
    pass

finally:
    consumer.close()