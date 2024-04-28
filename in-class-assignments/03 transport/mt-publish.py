from google.cloud import pubsub_v1
import json, base64, asyncio, multiprocessing

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

def pub_messages(myfi):
    vid = myfi[0]["VEHICLE_ID"]
    print(f'Starting {vid}')
    for i in range(len(myfi)):
        # print(f'VID: {vid} w/ Event stop: {myfi[i]["EVENT_NO_STOP"]}')
        res = base64.b64encode(str(myfi[i]).encode())
        future = publisher.publish(topic_path, res)
    print(f'{vid} COMPLETE!')
    
def getMulti(data_list, num_procs):
    p = multiprocessing.Pool(num_procs)
    p.imap(pub_messages, data_list)
    p.close()
    p.join()

if __name__ == "__main__":
    concurrences = 2
    data_list = [id_thirty_twenty, id_thirty_thirtysix]
    getMulti(data_list, concurrences)
    print(f'TASK COMPLETE!')