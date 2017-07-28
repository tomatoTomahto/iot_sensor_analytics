from kafka import KafkaProducer
import json

class KafkaConnection():
  def __init__(self, brokers, topic):
    self._kafka_producer = KafkaProducer(bootstrap_servers=brokers)
    self._topic = topic
    
  # Send payload (map) to kafka topic
  def send(self, payload):
    self._kafka_producer.send(self._topic, value=json.dumps(payload))
