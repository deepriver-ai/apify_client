import pika
from pika.exceptions import AMQPChannelError, AMQPConnectionError, AMQPError, ChannelClosed, ChannelError, ChannelWrongStateError, ConnectionClosed, ConnectionWrongStateError

RABBIT_EXCHANGE = os.getenv('RABBIT_EXCHANGE')
RABBIT_HOST = os.getenv('RABBIT_HOST')
RABBIT_PASSWORD = os.getenv('RABBIT_PASSWORD')
RABBIT_PORT = int(os.getenv('RABBIT_PORT'))
RABBIT_QUEUE = os.getenv('RABBIT_QUEUE')
RABBIT_USER = os.getenv('RABBIT_USER')
RABBIT_VIRTUAL_HOST = os.getenv('RABBIT_VIRTUAL_HOST')


class RMQ():

    def __init__(self):
        connection=None
        channel=None
        self.declarations()

    def declarations(self):
        self.connection = self.declare_connection()
        self.channel = self.declare_channel()

    def close_connections(self, ignore_exceptions=True):
        try:
            self.channel.close()
            self.connection.close()
        except Exception as e:
            print("Exception closing channel or connection to RabbitMQ")
            print(e)
            if not ignore_exceptions:
                raise e
            print("Continuing anyways...")

    def clear_connection(self):
        self.connection = None
        self.channel = None

    def declare_connection(self):
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    credentials=pika.PlainCredentials(RABBIT_USER, RABBIT_PASSWORD),
                    host=RABBIT_HOST,
                    port=RABBIT_PORT,
                    virtual_host=RABBIT_VIRTUAL_HOST
                ))
        except Exception as e:
            print("Exception declaring connection to RabbitMQ")
            print(e)
            raise e
        return connection

    def declare_channel(self):
        try:
            channel = self.connection.channel()
            channel.exchange_declare(exchange=RABBIT_EXCHANGE, exchange_type='direct', durable=True)
            channel.queue_declare(queue=RABBIT_QUEUE, durable=True)
            channel.queue_bind(exchange=RABBIT_EXCHANGE, queue=RABBIT_QUEUE)
        except Exception as e:
            print("Exception declaring channel, queue, or binding them.")
            print(e)
            raise e
        return channel

    def publish(self, message):
        try:
            self.channel.basic_publish(exchange=RABBIT_EXCHANGE, routing_key=RABBIT_QUEUE, body=message)
        except (AMQPChannelError, AMQPConnectionError, AMQPError, ChannelClosed, ChannelError, ChannelWrongStateError, ConnectionClosed, ConnectionWrongStateError):
            try:
                self.close_connections()
                self.clear_connection()
                self.declarations()
                self.publish(message)
            except Exception as e:
                raise e
        except Exception as e:
            raise e
        return None

    def close_client(self):
        self.close_connections(ignore_exceptions=False)
        self.clear_connection()

rmq = RMQ()

def publish(message):
    try:
        rmq.publish(message)
    except Exception as e:
        raise e
    return

def close_client():
    try:
        rmq.close_connections()
    except Exception as e:
        raise e

