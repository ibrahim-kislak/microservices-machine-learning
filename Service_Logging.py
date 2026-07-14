import rabbitmq_manager as rmq
import json
import os , sys
RABBIT = rmq.rabbitmq_service
channel=RABBIT.get_channel()
channel.exchange_declare(exchange='payment_exchange', exchange_type='fanout', durable=True)
channel.queue_declare(queue='logging_queue', durable=True)
channel.queue_bind(exchange='payment_exchange', queue='logging_queue')

def callback(ch, method, properties, body):
    body_str = body.decode('utf-8')
    payment_details = json.loads(body_str)
    print(f"Received payment details: {payment_details}")
    channel.basic_ack(delivery_tag=method.delivery_tag)
    
channel.basic_consume(queue='logging_queue', on_message_callback=callback)
print("Waiting for messages. To exit press CTRL+C")
try:
    channel.start_consuming()
except KeyboardInterrupt:
    try:
        sys.exit(0)
    except SystemExit:
        os._exit(0) 