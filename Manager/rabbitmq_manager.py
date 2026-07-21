import os

import pika

class RabbitMQManager:
    def __init__(self, host=None):
        self.host = host or os.getenv('RABBITMQ_HOST', 'localhost')
        self.connection = None
        self.channel = None

    def connect(self):
        
        if self.connection is None or self.connection.is_closed:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
            self.channel = self.connection.channel()
            
        
    def get_channel(self):
        if self.channel is None or self.channel.is_closed:
            self.connect()
        return self.channel

    def publish_message(self, message: str, routing_key: str, exchange: str = ''):
  
        ch = self.get_channel()  
        
        ch.basic_publish (
            exchange=exchange,
            routing_key=routing_key,  
            body=message,
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent
            ))
        

    def close_connection(self):
        
        if self.connection and  self.connection.is_open:
            self.connection.close()
            print("RabbitMQ connection closed.")

rabbitmq_service=RabbitMQManager()