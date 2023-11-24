from kafka_producer import Producer
from threading import Thread
from pyspark.sql import SparkSession
from connector import kafka_to_mongodb_pipeline


class BaseService:
    
    def __init__(self):
        
        self.spark = SparkSession.builder \
            .appName("KafkaToMongoDB") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
            .getOrCreate()
        
        self.inbound_internal_producer = Producer('InboundInternal')
        self.outbound_internal_producer = Producer('OutboundInternal')
        self.init_consumers()

    def init_consumers(self):
        ie_thread = Thread(target=kafka_to_mongodb_pipeline,args=[self.spark, "InboundExternal"])
        ii_thread = Thread(target=kafka_to_mongodb_pipeline,args=[self.spark, "InboundInternal"])
        
        ie_thread.start()
        ii_thread.start()

    def consume_message(self, consumer):
        consumer.consume_message()

    def send_inbound_internal_message(self, message):
        self.inbound_internal_producer.send_message(message=message)
    
    def send_outbound_internal_message(self, message):
        self.outbound_internal_producer.send_message(message=message)

