from threading import Thread
from base_customer import BaseCustomer
from kafka_consumer import Consumer

class CustomerTwo(BaseCustomer):
    def __init__(self):
        super().__init__()
        self.outbound_external_consumer = Consumer("OutboundExternal")
        self.init_oe_consumer()
    
    def init_oe_consumer(self):
        oe_thread = Thread(target=self.outbound_external_consumer.consume_message, args=[])
        oe_thread.start()

if __name__ == '__main__':
    customer = CustomerTwo()
    customer.send_inbound_external_message("Hey Atlan, this is customer 2")
