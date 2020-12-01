"""
Master component of spider. Assign download tasks into working directory.

Fetch top 500 sites for every country in https://www.alexa.com/topsites/countries, then send them to the RabbitMQ broker
for the workers to handle the downloading. Also creates the subdirectories for the workers to download into.
"""
from typing import Dict, List
import pika
import logging
import config as cfg
import os
import json
import sys

logger = logging.getLogger('master')
logging.basicConfig(level=cfg.logging['base_level'])
logger.setLevel(cfg.logging['script_level'])


def get_tasks() -> Dict[str, List[str]]:
    """
    Get a mock list of tasks.

    The task list is a dict with country codes as keys and a list of URLs as values.
    """
    logger.info('Got task list.')
    return {
        'RO': [
            'https://google.com',
            'https://google.ro',
            'https://yahoo.com',
        ],
        'UK': [
            'https://google.com',
            'https://google.co.uk',
            'https://microsoft.com',
        ],
    }


def distribute(tasks: Dict[str, List[str]]):
    """Send each task onto the broker's queue as a JSON string."""
    try:
        with pika.BlockingConnection(pika.ConnectionParameters(
            host=cfg.broker['host'] or 'localhost'
        )) as conn:
            logger.info('Connected to broker, starting task distribution')
            ch = conn.channel()

            # create queue if it doesn't exist, purge it if it does
            ch.queue_declare(cfg.broker['queue'], durable=True)
            logger.debug('Created queue')
            ch.queue_purge(cfg.broker['queue'])
            logger.debug('Purged queue')

            root_path = os.getcwd()
            for country, sites in tasks.items():
                country_path = os.path.join(root_path, country)
                if not os.path.exists(country_path):
                    os.mkdir(country_path)

                for site in sites:
                    message = {'url': site, 'path': country_path}
                    message_json = json.dumps(message)
                    ch.basic_publish(
                        exchange='',
                        routing_key=cfg.broker['queue'],
                        body=bytes(message_json, 'UTF-8'),
                        properties=pika.BasicProperties(
                            delivery_mode=2,  # make message persistent
                        )
                    )
                    logger.info('Wrote (%r, %r) to queue', country, site)
    except pika.exceptions.AMQPConnectionError as e:
        logger.debug('AMQP Connection Error: %r', e)
        logger.critical('Cannot connect to message broker, aborting')
        sys.exit(1)
    logger.info('All tasks distributed')


def main():
    tasks = get_tasks()
    distribute(tasks)


if __name__ == '__main__':
    main()
