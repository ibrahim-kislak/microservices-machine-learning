import Manager.rabbitmq_manager as rmq
import json
import time
import Manager.Redis_Manager as redis
REDIS = redis.redis_service
RABBIT = rmq.rabbitmq_service
time.sleep(10)  # Wait for RabbitMQ to be ready
channel=RABBIT.get_channel()
channel.exchange_declare(exchange='prediction_exchange', exchange_type='topic', durable=True)
channel.queue_declare(queue='prediction_queue', durable=True)
channel.queue_declare("logging_queue",durable=True)
channel.queue_bind(exchange='prediction_exchange', queue='prediction_queue',routing_key="hospital.*.prediction")
channel.queue_bind(exchange="prediction_exchange", queue="logging_queue",routing_key="hospital.#")
patients = [
    {
        "patient_id": "P_2001",
        "gender": "Male",
        "age": 80.0,
        "hypertension": 0,
        "heart_disease": 1,
        "ever_married": "Yes",
        "work_type": "Private",
        "Residence_type": "Rural",
        "avg_glucose_level": 105.92,
        "bmi": 32.5,
        "smoking_status": "never smoked"
    },
    {
        "patient_id": "P_2002",
        "gender": "Female",
        "age": 49.0,
        "hypertension": 0,
        "heart_disease": 0,
        "ever_married": "Yes",
        "work_type": "Private",
        "Residence_type": "Urban",
        "avg_glucose_level": 171.23,
        "bmi": 34.4,
        "smoking_status": "smokes"
    },
    {
        "patient_id": "P_2003",
        "gender": "Female",
        "age": 79.0,
        "hypertension": 1,
        "heart_disease": 0,
        "ever_married": "Yes",
        "work_type": "Self-employed",
        "Residence_type": "Rural",
        "avg_glucose_level": 174.12,
        "bmi": 24.0,
        "smoking_status": "never smoked"
    },
    {
        "patient_id": "P_2004",
        "gender": "Female",
        "age": 69.0,
        "hypertension": 0,
        "heart_disease": 0,
        "ever_married": "No",
        "work_type": "Private",
        "Residence_type": "Urban",
        "avg_glucose_level": 94.39,
        "bmi": 22.8,
        "smoking_status": "never smoked"
    },
    {
        "patient_id": "P_2005",
        "gender": "Female",
        "age": 61.0,
        "hypertension": 0,
        "heart_disease": 1,
        "ever_married": "Yes",
        "work_type": "Govt_job",
        "Residence_type": "Rural",
        "avg_glucose_level": 120.46,
        "bmi": 36.8,
        "smoking_status": "smokes"
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
        
        