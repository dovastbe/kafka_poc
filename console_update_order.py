import json
from kafka import KafkaConsumer


consumer = KafkaConsumer('orders',
                         group_id="console",
                         bootstrap_servers='localhost:9092')

tower_host = "http://localhost"
tower_user = "admin"
tower_token = "Mu21PadLaHLU3fUmh4IbM4vabs5bqx"

print("Console - Consumer now listening...")
while True:
    for message in consumer:
        consumed_message = json.loads(message.value.decode())
        if consumed_message["task_status"] != "pending":
                    print("-------------------")
                    print("Consuming message...")
                    print(message)
                    print("")     
                    print("Processing message as task_state is {} for order {}.".format(consumed_message["task_status"], consumed_message["order_id"]))
                    print("")

