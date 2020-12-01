import sys
import pika
import logging
import config as cfg

logger = logging.getLogger('worker')
logging.basicConfig(level=cfg.logging['base_level'])
logger.setLevel(cfg.logging['script_level'])


def callback(
        ch: pika.adapters.blocking_connection.BlockingChannel,
        method: pika.spec.Basic.Deliver,
        properties: pika.spec.BasicProperties,
        body: bytes
):
    """
    Handle task received from queue.

    Deserialize the JSON body, download the site in the right location,
    and then ACK the task's completion to the message broker.

    The signature is the specific one accepted by the basic_consume function
    for its `callback` argument.
    """
    pass


def main():
    logger.info('Starting task handling, press CTRL+C to abort')
    try:
        with pika.BlockingConnection(pika.ConnectionParameters(
            host=cfg.broker['host'] or 'localhost'
        )) as conn:
            ch = conn.channel()

            # create queue if it doesn't exist, purge it if it does
            ch.queue_declare(cfg.broker['queue'], durable=True)
            ch.basic_qos(prefetch_count=1)
            ch.basic_consume(cfg.broker['queue'], callback)
    except KeyboardInterrupt as e:
        logger.info('Keyboard interrupt detected, aborting')
    except pika.exceptions.AMQPConnectionError as e:
        logger.debug('AMQP Connection Error: %r', e)
        logger.critical('Cannot connect to message broker, aborting')
        sys.exit(1)


if __name__ == '__main__':
    main()
