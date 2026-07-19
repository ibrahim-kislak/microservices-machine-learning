import Manager.rabbitmq_manager as rmq
import json 
import os , sys
#import h2o
#h2o.init()
MODEL_PATH="/home/baris/python_microservice/Models/GLM_1_AutoML_1_20260716_180417"
#model=h2o.load_model(MODEL_PATH)
RABBIT=rmq.rabbitmq_service

channel=RABBIT.get_channel()

channel.exchange_declare(exchange='prediction_exchange', exchange_type='topic', durable=True)
channel.queue_declare(queue='prediction_queue', durable=True)
channel.queue_bind(exchange='prediction_exchange', queue='prediction_queue',routing_key="hospital.*.prediction")  

def callback (ch,method,properties,body):
    body_str=body.decode('utf-8')
    prediction_details=json.loads(body_str)
    
    print(f"Received prediction details: {prediction_details}")
    #h2o_format_data = {k: [v] for k, v in patient_details.items()}
    #predict=model.predict(h2o.H2OFrame(h2o_format_data))
    #pred_df= predict.as_data_frame()
    #p1_prediction = float(pred_df["p1"].iloc[0])
    #risk_durumu = p1_olasiligi > 0.104433 # Notebook'ta bulduğumuz ideal eşik
    #print(f"[!] TAHMİN SONUCU -> Riskli mi?: {risk_durumu} | İhtimal: %{p1_olasiligi*100:.2f}")
    channel.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_consume(queue='prediction_queue', on_message_callback=callback)
print("Waiting for messages. To exit press CTRL+C")

try:
    channel.start_consuming()
except KeyboardInterrupt:
    try:
        sys.exit(0) 
    except SystemExit:
        os._exit(0)