import subprocess
import sys
import json
from kafka import KafkaConsumer


consumer = KafkaConsumer('order_details',
                         group_id="orders",
                         bootstrap_servers='localhost:29092')

tower_host = "http://localhost"
tower_user = ""
tower_token = ""

print("Gonna start listening")
while True:
    for message in consumer:
        print("Here is a message..")
        print(message)
        print("Processing order...")
        consumed_message = json.loads(message.value.decode())
        extra_vars = json.dumps(consumed_message["extra_vars"])
        print(extra_vars)
        result = subprocess.run(
            ["awx", "--conf.host", tower_host, "--conf.token", tower_token, "--conf.username", tower_user, "job_templates", "launch", "7", "--extra_vars", extra_vars], capture_output=True, text=True)
        json_result = json.loads(result.stdout)
        print(json_result["id"])
        #print("stdout:", result.stdout)
        #print("stderr:", result.stderr)
