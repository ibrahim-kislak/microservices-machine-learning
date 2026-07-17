import Manager.rabbitmq_manager as rmq
import json
import Manager.Redis_Manager as redis
REDIS = redis.redis_service
RABBIT = rmq.rabbitmq_service

channel=RABBIT.get_channel()
channel.exchange_declare(exchange='prediction_exchange', exchange_type='fanout', durable=True)
channel.queue_declare(queue='prediction_queue', durable=True)
channel.queue_bind(exchange='prediction_exchange', queue='prediction_queue')

channel.queue_declare("logging_queue",durable=True)
channel.queue_bind(exchange="prediction_exchange", queue="logging_queue")
patients = [
    {
        "patient_id": "P_1001",
        "age": 67.0,
        "bmi": 36.6,
        "hypertension": 1,
        "smoking_status": "formerly smoked",
        "work_type": "Private",
        "gender": "Male",
        "Residence_type": "Urban"
    },
    {
        "patient_id": "P_1002",
        "age": 45.0,
        "bmi": 22.1,
        "hypertension": 0,
        "smoking_status": "never smoked",
        "work_type": "Govt_job",
        "gender": "Female",
        "Residence_type": "Rural"
    },
    {
        "patient_id": "P_1003",
        "age": 78.0,
        "bmi": 29.4,
        "hypertension": 1,
        "smoking_status": "smokes",
        "work_type": "Self-employed",
        "gender": "Male",
        "Residence_type": "Urban"
    }
]

def process_patient (patient):
    patient_id=patient["patient_id"]
    redis_key=f"stroke_prediction :{patient_id}"
    cached_status=REDIS.get_value(redis_key)
    
    if cached_status is not None:
        print(f"Cache Hit: {patient_id} status: {cached_status}")
    else: 
        print(f"Cache Miss: {patient_id} Sent to the machine to prediction...")
        msg=json.dumps(patient)
        RABBIT.publish_message(message=msg,exchange="prediction_exchange",routing_key="")
        REDIS.set_value(redis_key,"işleme alindi veya tahmin edildi",ttl_sec=360)
        
        