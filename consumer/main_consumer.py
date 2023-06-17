import json
from datetime import datetime


import psycopg2 as pg
from kafka import KafkaConsumer


from rcs import Checker 


def insert(connection, data, is_valid):
   insert_sql = f"""INSERT INTO request (email, phone_number, name, last_name, course_id, is_valid)
                    VALUES('{data['email']}',
                    '{data['phone_number']}',
                    '{data['name']}',
                    '{data['last_name']}',
                    {data['course_id']},
                    {is_valid});
   """
   connection.execute(insert_sql)






def cdc(from_connection, to_connection, checker):
   subscription = get_subscription()
   for message in from_connection:
       with to_connection.cursor() as to_cursor:
           try:
               value = json.loads(message.value.decode("utf-8"))
               insert(to_cursor, value, checker.check(value))
			   to_connection.commit()
           except Exception as e:
               print(e)
               to_connection.rollback()




if __name__ == '__main__':
   try:
       checker = Checker('request_to_course')
       consumer = KafkaConsumer('request_to_course',
                                bootstrap_servers=['51.250.2.37:29092'],
                                auto_offset_reset='earliest')


       with open("secret/dwh.txt") as file:
           database_info = file.read().replace('\n', ' ')


       with pg.connect(database_info) as to_connection:
           cdc(consumer, to_connection,checker)
       consumer.close()
   except Exception as e:
       print(e)
