from base_customer import BaseCustomer
from kafka_producer import Producer

class CustomerOne(BaseCustomer):
    def __init__(self):
        super().__init__()
        self.outbound_external_producer = Producer("OutboundExternal")
    
    def send_outbound_external_message(self, message):
        self.outbound_external_producer.send_message(message=message)
    

if __name__ == '__main__':
    
    customer = CustomerOne()
    customer.send_inbound_external_message("Hi Atlan, this is customer 1")
    customer.send_outbound_external_message("Hi downstream, this is customer 1")

