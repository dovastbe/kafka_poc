import json
import time

from kafka import KafkaProducer

ORDER_KAFKA_TOPIC = "order_details"
ORDER_LIMIT = 10

producer = KafkaProducer(bootstrap_servers="localhost:29092")

print("Geneating order after 5 seconds")
print("Will generate one unique order every 10 seconds")
time.sleep(5)

for i in range(1, ORDER_LIMIT):
    order = str(i)
    data = {
        "order_id": i,
        "extra_vars": {
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
    print(f"Done Sending..{i}")
    time.sleep(10)
