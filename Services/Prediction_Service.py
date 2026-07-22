import Manager.rabbitmq_manager as rmq
import json 
import os , sys
import h2o
h2o.init()
MODEL_PATH = "/app/Models/GLM_1_AutoML_1_20260716_180417"
model=h2o.load_model(MODEL_PATH)
RABBIT=rmq.rabbitmq_service

channel=RABBIT.get_channel()

channel.exchange_declare(exchange='prediction_exchange', exchange_type='topic', durable=True)
channel.queue_declare(queue='prediction_queue', durable=True)
channel.queue_bind(exchange='prediction_exchange', queue='prediction_queue',routing_key="hospital.*.prediction")  

def callback (ch,method,properties,body):
    try:
        body_str = body.decode('utf-8')
        patient_details = json.loads(body_str)
        
        h2o_format_data = {k: [v] for k, v in patient_details.items()}
        
        predict = model.predict(h2o.H2OFrame(h2o_format_data))
        p1_prediction = float(predict["p1"][0, 0])
        
        is_at_risk = p1_prediction > 0.104433  
        print(f"[!] PREDICTION RESULT -> At Risk?: {is_at_risk} | Probability: {p1_prediction*100:.2f}%")
        
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except json.JSONDecodeError:
        print("[-] ERROR: Incoming message is not valid JSON.")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        
    except Exception as e:
        print(f"[-] UNEXPECTED ERROR: {str(e)}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

channel.basic_consume(queue='prediction_queue', on_message_callback=callback)
print("Waiting for messages. To exit press CTRL+C")

try:
    channel.start_consuming()
except KeyboardInterrupt:
    try:
        sys.exit(0) 
    except SystemExit:
        os._exit(0)