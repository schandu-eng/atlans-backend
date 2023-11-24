from kafka_consumer import Consumer
from kafka_producer import Producer
from threading import Thread

class BaseCustomer:

    def __init__(self):
        self.inbound_external_producer = Producer("InboundExternal")
        self.outbound_internal_consumer = Consumer("OutboundInternal")
        self.init_consumer()
    
    def init_consumer(self):
        oi_thread = Thread(target=self.outbound_internal_consumer.consume_message, args=[])
        oi_thread.start()

    def send_inbound_external_message(self, message):
        self.inbound_external_producer.send_message(message=message)
    
