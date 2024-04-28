from google.cloud import pubsub_v1
import json, base64

project_id = "data-eng-419218"
topic_id = "my-topic"

publisher = pubsub_v1.PublisherClient()

# The `topic_path` method creates a fully qualified identifier
# in the form `projects/{project_id}/topics/{topic_id}`
topic_path = publisher.topic_path(project_id, topic_id)

id_thirty_twenty = open('id3020.json')
id_thirty_thirtysix = open('id3036.json')
id_thirty_twenty = json.load(id_thirty_twenty)
id_thirty_thirtysix = json.load(id_thirty_thirtysix)


for i in range(len(id_thirty_twenty)):
    res = base64.b64encode(str(id_thirty_twenty[i]).encode())
    future = publisher.publish(topic_path, res)

for i in range(len(id_thirty_thirtysix)):
    res = base64.b64encode(str(id_thirty_thirtysix[i]).encode())
    future = publisher.publish(topic_path, res)

