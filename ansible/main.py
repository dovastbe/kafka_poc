from flask import Flask, request, Response

import json
from kafka import KafkaConsumer
from kafka import KafkaProducer


JOBS_KAFKA_TOPIC = "submitted_jobs"
RUNNING_JOBS_KAFKA_TOPIC = "running_jobs"
FAILED_JOBS_KAFKA_TOPIC = "failed_jobs"
SUCCESSFUL_JOBS_KAFKA_TOPIC = "successful_jobs"
UNHANDLED_JOBS_KAFKA_TOPIC = "unhandled_jobs"

#producer = KafkaProducer(bootstrap_servers="localhost:29092")

app = Flask(__name__)


@app.route('/webhook', methods=['POST'])
def respond():
    print(request.json)
    job = request.json
    print("Job {} is {}".format(job["id"], job["status"]))
    extra_vars = json.loads(job["extra_vars"])
    print(extra_vars["AppCode"])
    if job["status"] == 'running':
        KAFKA_TOPIC = RUNNING_JOBS_KAFKA_TOPIC
    elif job["status"] == 'successful':
        KAFKA_TOPIC = SUCCESSFUL_JOBS_KAFKA_TOPIC
        print("Moving request to the next workflow phase - pre_validated")
    elif job["status"] == 'failed':
        KAFKA_TOPIC = FAILED_JOBS_KAFKA_TOPIC
        print("Moving request to the next workflow phase - failed")
    else:
        print("Unknown job status")
        #producer.send(UNHANDLED_JOBS_KAFKA_TOPIC, json.dumps(job).encode("utf-8"))
        return Response(status=500)
    print("Sending producer mesaage for job {} is {} to the kakfa topic {}".format(
        job["id"], job["status"], KAFKA_TOPIC))
    #producer.send(KAFKA_TOPIC, json.dumps(job).encode("utf-8"))
    return Response(status=200)
