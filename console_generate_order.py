import json
import time
from kafka import KafkaProducer
from datetime import datetime
import random

ORDER_KAFKA_TOPIC = "orders"
ORDER_LIMIT = 10

producer = KafkaProducer(bootstrap_servers="localhost:9092")

print("Console - Geneating order after 5 seconds")
print("Generate one unique order request every 20 seconds")
time.sleep(5)

# Itterate between task_systems
task_systems = ["terraform","ansible"]

for i in range(1, ORDER_LIMIT):
    order = str(i)
    task_system = random.choice(task_systems)
    data = {
        "order_id": i,
        "order_date": str(datetime.now()),
        "task_status": "pending",
        "task_system": task_system,
        "extra_vars": {
            "order_id": i,
            "workflow_phase": "new",
            "Admins": [
                "user1",
                "user2",
            ],
            "AppCode": "INF",
            "AppName": "Infrastructure",
            "Email": "jbloggs@domain.com",
            "FirstName": "Joe",
            "LastName": "Bloggs",
            "members": [
                "user3",
                "user4",
            ]
        }
    }
    producer.send(ORDER_KAFKA_TOPIC, key=order.encode("utf-8"),
                  value=json.dumps(data).encode("utf-8"))
    print("Sent order_id {} for task_system {}".format(order, task_system))
    time.sleep(20)
