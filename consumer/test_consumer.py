import json
from datetime import datetime

from kafka import KafkaConsumer

def messages(from_connection, checker):
   subscription = get_subscription()
   for message in from_connection:
      print(message)


if __name__ == '__main__':
   try:
       checker = Checker('request_to_course')
       consumer = KafkaConsumer('request_to_course',
                                bootstrap_servers=['51.250.2.37:29092'],
                                auto_offset_reset='earliest')
       messages(consumer, to_connection,checker)

