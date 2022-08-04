from flask import Flask, request, Response

import json
from kafka import KafkaConsumer
from kafka import KafkaProducer


KAFKA_TOPIC = "orders"


producer = KafkaProducer(bootstrap_servers="localhost:9092")

app = Flask(__name__)


@app.route('/webhook', methods=['POST'])
def respond():
    print(request.json)
    job = request.json
    if "id" in job:
        print("-------------------")
        print("POST Notfication from Tower...")
        extra_vars = json.loads(job["extra_vars"])
        print("Ansible Tower job {} for order {} is {}".format(job["id"], extra_vars["order_id"],  job["status"]))
        print("Sending message back to message stream")
        # build the messsage 
        order = str(extra_vars["order_id"])
        data = {
            "order_id": order,
            "task_status": job["status"],
            }
        producer.send(KAFKA_TOPIC, key=order.encode("utf-8"), value=json.dumps(data).encode("utf-8"))
    return Response(status=200)

app.run()