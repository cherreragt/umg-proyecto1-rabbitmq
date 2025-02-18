import json
import uuid
import random
from datetime import datetime
import pika
import sys
import time

queues = [
    {'name': 'project.callcenter', 'routing_key': 'routing-callcenter', 'operator': 'Operador Call Center'},
    {'name': 'project.app', 'routing_key': 'routing-app', 'operator': 'Operador App Móvil'},
    {'name': 'project.web', 'routing_key': 'routing-web', 'operator': 'Operador Página Web'}
]

exchangeName = 'project.ex.direct'

# Datos de ejemplo para generar pedidos aleatorios
clientes = ["Alice", "Bob", "Charlie", "David", "Eva"]
direcciones = [
    "Av. Siempre Viva 123",
    "Calle Falsa 456",
    "Boulevard de los Sueños 789",
    "Plaza Mayor 101",
    "Callejón del Beso 202"
]
tipos_pedido = ["Pizza", "Hamburguesa", "Sushi", "Ensalada", "Pasta"]
descripciones = [
    "Sin cebolla",
    "Extra queso",
    "Con doble ración de carne",
    "Poca sal",
    "Bien caliente"
]

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    channel.exchange_declare(
        exchange=exchangeName,
        exchange_type='direct',
        durable=True
    )

    for q in queues:
        channel.queue_declare(queue=q['name'], durable=True)
        channel.queue_bind(queue=q['name'], exchange=exchangeName, routing_key=q['routing_key'])

    print('Empresa FastDeliver')
    print("Iniciando generación de pedidos aleatorios cada 5 segundos...")

    try:
        while True:
            selected_queue = random.choice(queues)
            routing_key = selected_queue['routing_key']

            # Generar datos aleatorios para el pedido
            pedido_id = str(uuid.uuid4())
            fecha_hora = datetime.now().isoformat()
            cliente = random.choice(clientes)
            direccion = random.choice(direcciones)
            tipo_pedido = random.choice(tipos_pedido)
            descripcion = random.choice(descripciones)

            pedido = {
                "id": pedido_id,
                "fecha_hora": fecha_hora,
                "cliente": cliente,
                "direccion": direccion,
                "tipo_pedido": tipo_pedido,
                'estado': 'Generado',
                "descripcion": descripcion
            }

            json_mensaje = json.dumps(pedido)

            channel.basic_publish(
                exchange=exchangeName,
                routing_key=routing_key,
                body=json_mensaje,
                properties=pika.BasicProperties(
                    content_type='application/json',
                    delivery_mode=2
                )
            )

            print(f"\nMensaje JSON enviado al exchange '{exchangeName}' con routing key '{routing_key}'")
            print("Datos del pedido:")
            print(json_mensaje)

            time.sleep(5)

    except KeyboardInterrupt:
        print("\nSaliendo del programa...")
        connection.close()
        sys.exit(0)

if __name__ == "__main__":
    main()
