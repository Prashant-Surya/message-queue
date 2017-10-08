import random
from msgq.client import Producer

def publish_producer_one(queue, message):
    producer = Producer('127.0.0.1', 9996)
    producer.publish(queue, message)

num = random.randint(0,100)
message = 'User-{0} signed up'.format(num)
publish_producer_one('notifications', message)