import time

import pika


def main():
    connection = pika.BlockingConnection()
    channel = connection.channel()

    queue = "panta_test_queue"
    exchange = "amq.topic"
    routing_key = "panta-test"

    channel.queue_declare(queue)
    channel.queue_bind(queue, exchange, routing_key)

    # for i in range(0, 100):
    #     channel.basic_publish(exchange, routing_key, "Hello, World #{}!".format(i).encode("utf-8"))
    #     time.sleep(0.5)

    def callback(channel, method, properties, body):
        print(method.routing_key, body.decode("utf-8"))
        channel.basic_ack(method.delivery_tag)

    channel.basic_consume(queue, callback)
    channel.start_consuming()

if __name__ == "__main__":
    main()