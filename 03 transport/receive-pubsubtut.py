from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
import base64

project_id = "data-eng-419218"
subscription_id = "my-sub"

# Number of seconds the subscriber should listen for messages
timeout = 200.0

subscriber = pubsub_v1.SubscriberClient()

# The `subscription_path` method creates a fully qualified identifier
# in the form `projects/{project_id}/subscriptions/{subscription_id}`
subscription_path = subscriber.subscription_path(project_id, subscription_id)

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    res = base64.b64decode(message.data)
    # print(f"Received {message}.")

    # TODO json object properly from string?
    print(f"Data Result After Deocding: {res}.")
    message.ack()

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}..\n")

# Wrap subscriber in a 'with' block to automatically call close() when done.
with subscriber:
    try:
        # When `timeout` is not set, result() will block indefinitely,
        # unless an exception is encountered first.
        streaming_pull_future.result(timeout=timeout)
    except TimeoutError:
        streaming_pull_future.cancel()  # Trigger the shutdown.
        streaming_pull_future.result()  # Block until the shutdown is complete.
        