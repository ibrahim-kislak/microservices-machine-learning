import Manager.rabbitmq_manager as rmq
import json
import Manager.Redis_Manager as redis
REDIS = redis.redis_service
RABBIT = rmq.rabbitmq_service

channel=RABBIT.get_channel()
channel.exchange_declare(exchange='payment_exchange', exchange_type='fanout', durable=True)


payment_details = {
    "amount": 100.0,
    "payment_id": "pay_123456",
    "currency": "USD",
    "payment_method": "credit_card",
    "card_number": "4111111111111111",
    "is_paid": False}

def Process_payment (payment_details):
    payment_id = payment_details["payment_id"]
    redis_key = f"payment_status:{payment_id}"
    cached_status = REDIS.get_value(redis_key) 
    
    if cached_status is not None:
        print(f"[CACHE HIT] Payment with ID {payment_id} has already been processed. Cached status: {cached_status}")
        payment_details["is_paid"] = True
        RABBIT.publish_message(message=json.dumps(payment_details), exchange='payment_exchange', routing_key='')
        return
    else:
        print(f"[CACHE MISS] Processing payment with ID {payment_id}.")
        payment_details["is_paid"] = True
        message = json.dumps(payment_details)
        print("Payment processed successfully.")   
        
        RABBIT.publish_message(message=message, exchange='payment_exchange', routing_key='')
        REDIS.set_value(redis_key, "paid", ttl_sec=360)  
    
    
Process_payment(payment_details)
