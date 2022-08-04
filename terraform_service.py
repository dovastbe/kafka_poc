import subprocess
import sys
import json
from kafka import KafkaConsumer


consumer = KafkaConsumer('orders',
                         group_id="terraform_service",
                         bootstrap_servers='localhost:9092')


print("Terraform Service - Consumer now listening...")
while True:
    for message in consumer:
        consumed_message = json.loads(message.value.decode())
        if consumed_message["task_status"] == "pending" and consumed_message["task_system"] == "terraform":
            print("-------------------")
            print("Consuming message...")
            print(message)
            print("")     
            print("Processing message as state is {}.".format(consumed_message["task_status"]))
            print("Launching Terraform job with payload...")
            print("Terraform  job launched for order {}".format(consumed_message["order_id"]))
            print("")

