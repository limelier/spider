"""
Master component of spider. Assign download tasks into working directory.

Fetch top 50 sites for every country in https://www.alexa.com/topsites/countries, then send them to the RabbitMQ broker
for the workers to handle the downloading. Also creates the subdirectories for the workers to download into.
"""
import pika
import logging
import config as cfg
import os
import json
import sys
import scraping

logger = logging.getLogger('master')
logging.basicConfig(level=cfg.logging['base_level'])
logger.setLevel(cfg.logging['script_level'])


def init_queue(ch: pika.adapters.blocking_connection.BlockingChannel):
    """
    Create queue if it doesn't exist, purge it if it does.

    :param ch: the channel to the message broker
    """
    ch.queue_declare(cfg.broker['queue'], durable=True)
    logger.debug('Created queue')
    ch.queue_purge(cfg.broker['queue'])
    logger.debug('Purged queue')


def assign_tasks(ch: pika.adapters.blocking_connection.BlockingChannel):
    """
    Web scrape Alexa Top Sites, creating tasks to send to the workers by the given channel.

    :param ch: the channel to the message broker
    """
    logger.info('Scraping country list')
    country_tuples = scraping.find_countries()
    logger.info('Got list of countries, scraping individual pages')

    root_path = os.getcwd()

    for country, href in country_tuples:
        sites = scraping.find_sites(href)
        logger.info('Got list of sites for %r', country)

        country_path = os.path.join(root_path, country)
        if not os.path.exists(country_path):
            os.mkdir(country_path)
        logger.info('Created path %r', country_path)

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
            logger.debug('Wrote (%r, %r) to queue', country, site)
        logger.info('Wrote all tasks for %r', country)


def main():
    """Web scrape Alexa Top Sites to create tasks to send to the message broker."""
    try:
        with pika.BlockingConnection(pika.ConnectionParameters(
                host=cfg.broker['host'] or 'localhost'
        )) as conn:
            logger.info('Connected to broker, starting task distribution')
            ch = conn.channel()
            init_queue(ch)
            assign_tasks(ch)
    except pika.exceptions.AMQPConnectionError as e:
        logger.debug('AMQP Connection Error: %r', e)
        logger.critical('Cannot connect to message broker, aborting')
        sys.exit(1)
    logger.info('All tasks distributed')


if __name__ == '__main__':
    main()
