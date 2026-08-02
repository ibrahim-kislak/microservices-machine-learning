import Manager.rabbitmq_manager as rmq
import json
import os , sys,time
import logging
import Manager.Prometheus_Manager as prom
time.sleep(10)
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
channel.exchange_declare(exchange='prediction_exchange', exchange_type='topic', durable=True)
channel.queue_declare(queue='logging_queue', durable=True)
channel.queue_bind(exchange='prediction_exchange', queue='logging_queue',routing_key="hospital.#")
channel.queue_declare(queue='prediction_queue', durable=True)
channel.queue_bind(exchange='prediction_exchange', queue='prediction_queue',routing_key="hospital.*.prediction")
PROMETHEUS=prom.prometheus_service
PROMETHEUS.start_server()
def callback(ch, method, properties, body):
    PROMETHEUS.increment_active_prediction()
    try:
        with PROMETHEUS.track_prediction_duration():
            body_str = body.decode('utf-8')
            patient_details = json.loads(body_str)
            patient_id=patient_details.get("patient_id","NaN")
            logging.info(f"New Patient (ID): {patient_id} ")
            logging.debug(f"Incoming Data: {patient_details}")
            PROMETHEUS.record_prediction(status="success")

    except json.JSONDecodeError as e :
        logging.error(f"Data must be a json file {str(e)}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        PROMETHEUS.record_prediction("json_decode_error")
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        PROMETHEUS.record_prediction("error")
        
    finally:
        #channel.basic_ack(delivery_tag=method.delivery_tag)
        PROMETHEUS.decrement_active_prediction()
    
channel.basic_consume(queue='logging_queue', on_message_callback=callback)
print("Waiting for messages. To exit press CTRL+C")
try:
    channel.start_consuming()
except KeyboardInterrupt:
    try:
        sys.exit(0)
    except SystemExit:
        os._exit(0) 