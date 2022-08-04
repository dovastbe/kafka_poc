import subprocess
import sys
import json
from kafka import KafkaConsumer


consumer = KafkaConsumer('orders',
                         group_id="ansible_service",
                         bootstrap_servers='localhost:9092')

tower_host = "http://localhost"
tower_user = "admin"
tower_token = "Mu21PadLaHLU3fUmh4IbM4vabs5bqx"

print("Ansible Service - Consumer now listening...")
while True:
    for message in consumer:
        consumed_message = json.loads(message.value.decode())
        if consumed_message["task_status"] == "pending" and consumed_message["task_system"] == "ansible":
            print("-------------------")
            print("Consuming message...")
            print(message)
            print("")     
            print("Processing message as state is {}.".format(consumed_message["task_status"]))
            print("Launching Ansible Tower job with payload...")
            extra_vars = json.dumps(consumed_message["extra_vars"])
            print(extra_vars)
            result = subprocess.run(
                ["awx", "--conf.host", tower_host, "--conf.token", tower_token, "--conf.username", tower_user, "job_templates", "launch", "7", "--extra_vars", extra_vars], capture_output=True, text=True)
            json_result = json.loads(result.stdout)
            print("Ansible tower job launched, job ID {} for order {}".format(json_result["id"], consumed_message["order_id"]))
            print("")

