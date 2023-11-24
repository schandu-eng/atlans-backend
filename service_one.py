from base_service import BaseService


class ServiceOne(BaseService):

    def __init__(self):
        super().__init__()


if __name__ == '__main__':
    
    service = ServiceOne()
    service.send_inbound_internal_message("Hi Atlan, from service 1")
    service.send_outbound_internal_message("Hi Customer, from service 1")