import multiprocessing
from google.cloud import pubsub_v1

project_id = "your-project-id"
subscription_id = "your-subscription-id"

time

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    res = base64.b64decode(message.data)
    print(f"Data Result After Decoding: {res}.")
    message.ack()

def rcv_messages(subscriber):
    subscriber = pubsub_v1.SubscriberClient()  # Create a new subscriber client instance
    subscription_path = subscriber.subscription_path(project_id, subscription_id)
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

def getMulti(data_list, num_procs):
    p = multiprocessing.Pool(num_procs)
    p.map(rcv_messages, data_list)
    p.close()
    p.join()

if __name__ == "__main__":
    concurrences = 2
    data_list = [None] * concurrences  # Create a list of None values since rcv_messages doesn't need any arguments
    getMulti(data_list, concurrences)
    print(f'TASK COMPLETE!')
