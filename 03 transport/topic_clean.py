from google.cloud import pubsub_v1

timeout = 200.0

project_id = "data-eng-419218"
subscription_id = "my-sub"

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

def ack_all(message: pubsub_v1.subscriber.message.Message) -> None:
    message.ack()

streaming_pull_future = subscriber.subscribe(subscription_path, callback=ack_all)
# Wrap subscriber in a 'with' block to automatically call close() when done.
with subscriber:
    try:
        # When `timeout` is not set, result() will block indefinitely,
        # unless an exception is encountered first.
        streaming_pull_future.result(timeout=timeout)
    except TimeoutError:
        streaming_pull_future.cancel()  # Trigger the shutdown.
        streaming_pull_future.result()  # Block until the shutdown is complete.