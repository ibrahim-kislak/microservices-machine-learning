import rabbitmq_manager as rmq
import json
RABBIT = rmq.rabbitmq_service
channel=RABBIT.get_channel()
channel.exchange_declare(exchange='payment_exchange', exchange_type='fanout', durable=True)


payment_details = {
    "amount": 100.0,
    "currency": "USD",
    "payment_method": "credit_card",
    "card_number": "4111111111111111",
    "is_paid": False}

def Process_payment (payment_details):
    
    if payment_details["is_paid"]:
        print("Payment has already been processed.")
        
        RABBIT.publish_message(message=json.dumps(payment_details),exchange='payment_exchange' ,routing_key='')
        return  
    else :  
        message = json.dumps(payment_details)
        print("Payment processed successfully.")   
        
        RABBIT.publish_message(message=message, exchange='payment_exchange', routing_key='')
    
Process_payment(payment_details)
