import Manager.rabbitmq_manager as rmq
import json
import os , sys
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] - %(message)s',
    handlers=[
    logging.FileHandler("system_logs.log",encoding='utf-8'),
    logging.StreamHandler(sys.stdout)
    ]
)

RABBIT = rmq.rabbitmq_service
channel=RABBIT.get_channel()
channel.exchange_declare(exchange='prediction_exchange', exchange_type='fanout', durable=True)
channel.queue_declare(queue='logging_queue', durable=True)
channel.queue_bind(exchange='prediction_exchange', queue='logging_queue')
channel.queue_declare(queue='prediction_queue', durable=True)
channel.queue_bind(exchange='prediction_exchange', queue='prediction_queue')
def callback(ch, method, properties, body):
    try:
        body_str = body.decode('utf-8')
        patient_details = json.loads(body_str)
        patient_id=patient_details.get("id","NaN")
        logging.info(f"New Patient (ID): {patient_id} ")
        logging.debug(f"Incoming Data: {patient_details}")
    except json.JSONDecodeError as e :
        logging.error(f"Data must be a json file {str(e)}")
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")
        
    finally:
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