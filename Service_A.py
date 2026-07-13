import rabbitmq_manager as rmq
import json
RABBIT = rmq.rabbitmq_service

payment_details = {
    "amount": 100.0,
    "currency": "USD",
    "payment_method": "credit_card",
    "card_number": "4111111111111111",
    "is_paid": False}

def Process_payment (payment_details):
    
    if payment_details["is_paid"]:
        print("Payment has already been processed.")
        return  
    else :  
        message = json.dumps(payment_details)
        print("Payment processed successfully.")   
        
        RABBIT.publish_message(message=message, routing_key='payment_queue')
    
Process_payment(payment_details)
