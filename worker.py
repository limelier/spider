"""
Worker component of spider. Download sites as assigned by message broker.

Run continuously, listening to the message broker for new tasks. Handle tasks by deserializing them from JSON, and
downloading the given website into the target directory using a filename of the form '<hostname>.html' (for example,
'google.com.html').

Stop with CTRL-C.
"""
import sys
import pika
import logging
import config as cfg
import json
import urllib.request as url_req
import os

logger = logging.getLogger('worker')
logging.basicConfig(level=cfg.logging['base_level'])
logger.setLevel(cfg.logging['script_level'])


def download_page(url: str, filename: str, directory: str):
    """
    Download a webpage, saving it into the given directory.
    :param url: the URL of the page that needs to be downloaded
    :param filename: the name of the HTML file that will be created
    :param directory: the directory to create the file into
    """
    response = url_req.urlopen(url)
    data = response.read()

    path = os.path.join(directory, filename)
    logger.info('Downloaded site, writing to file %r', path)
    with open(path, 'bw+') as file:
        file.write(data)


def callback(
        ch: pika.adapters.blocking_connection.BlockingChannel,
        method: pika.spec.Basic.Deliver,
        properties: pika.spec.BasicProperties,
        body: bytes
):
    """
    Use as callback arg for channel.start_consuming(); handles incoming tasks.

    Deserialize the body of the task, download the website and ACK the task's completion.
    """
    task = json.loads(body)
    url, directory = task['url'], task['path']
    logger.info('Downloading %r into %r', url, directory)
    filename = url.split('//')[1] + '.html'
    download_page(url, filename, directory)
    ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    """
    Run until SIGTERM, calling callback with every received task.
    """
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
            ch.start_consuming()
    except KeyboardInterrupt as e:
        logger.info('Keyboard interrupt detected, aborting')
    except pika.exceptions.AMQPConnectionError as e:
        logger.debug('AMQP Connection Error: %r', e)
        logger.critical('Cannot connect to message broker, aborting')
        sys.exit(1)


if __name__ == '__main__':
    main()
