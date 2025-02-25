import json

import pika
import os
import sys

exchangeName = 'project.ex.direct'
exchangeType = 'direct'

queues = [
    #{'name': 'project.callcenter', 'routing_key': 'routing-callcenter'},
    #{'name': 'project.app', 'routing_key': 'routing-app'},
    {'name': 'project.web', 'routing_key': 'routing-web'}
]

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    channel.exchange_declare(
        exchange=exchangeName,
        exchange_type=exchangeType,
        durable=True
    )

    for q in queues:
        channel.queue_declare(queue=q['name'], durable=True)
        channel.queue_bind(queue=q['name'], exchange=exchangeName, routing_key=q['routing_key'])

    def callback(ch, method, properties, body):
        try:
            mensaje = json.loads(body.decode())
            mensaje['estado'] = 'Procesado'
            mensaje_formateado = json.dumps(mensaje, indent=2, ensure_ascii=False)
            print(f" [x] Recibido en canal {method.routing_key}:\n{mensaje_formateado}\n")
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except json.JSONDecodeError:
            print(f" [x] Recibido en canal {method.routing_key}: {body.decode()}\n")
            ch.basic_ack(delivery_tag=method.delivery_tag)

    for q in queues:
        channel.basic_consume(queue=q['name'], on_message_callback=callback, auto_ack=False)

    channel.start_consuming()

    print("Broker was initialized")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Interrupted")
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
