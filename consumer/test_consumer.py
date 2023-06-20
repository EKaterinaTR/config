import json
from datetime import datetime

from kafka import KafkaConsumer

def messages(from_connection, checker):
   for message in from_connection:
      print(message)


if __name__ == '__main__':
   try:
       consumer = KafkaConsumer('request_to_course',
                                bootstrap_servers=['localhost:29092'],
                                auto_offset_reset='earliest')
       messages(consumer, to_connection,checker)

