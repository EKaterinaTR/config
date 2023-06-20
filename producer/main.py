import json
import sys
import random
from time import sleep

from kafka import KafkaProducer
from faker import Faker


def message():
    fake = Faker('ru_RU')
    request = {}
    request['email'] = fake.ascii_free_email()
    request['phone_number'] = fake.phone_number()
    request['source'] = fake.hostname()
    request['last_name'] = fake.last_name_female()
    request['name'] = fake.first_name_female()
    request['course'] = fake.random_sample(elements=('IT', 'IT POOL', 'JUMPING', 'Танго'), length=1)
    request['age'] = int(random.uniform(16, 100))
    return json.dumps(request).encode('ascii')


if __name__ == '__main__':
    if len(sys.argv) > 1:
        sleeptime = float(sys.argv[1]) / 1000.0
    else:
        sleeptime = 2
    producer = KafkaProducer(bootstrap_servers=['localhost:29092'])
    try:
        while True:
            producer.send('request_to_course', message())
            producer.flush()
            sleep(sleeptime)
    except Exception as e:
        producer.close()
        raise e
