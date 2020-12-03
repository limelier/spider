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
import requests
import os

logger = logging.getLogger('worker')
logging.basicConfig(level=cfg.logging['base_level'])
logger.setLevel(cfg.logging['script_level'])


def add_www(url):
    protocol, host = url.split('://')
    return '{}://www.{}'.format(protocol, host)


def download_page(url: str, filename: str, directory: str, verify: bool = True):
    """
    Download a webpage, saving it into the given directory.

    :param url: the URL of the page that needs to be downloaded
    :param filename: the name of the HTML file that will be created
    :param directory: the directory to create the file into
    :param verify: verify SSL certificate
    """
    try:
        # use different user-agent to avoid 403: Forbidden
        # time out after 2 seconds
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, verify=verify, timeout=5)
    except requests.exceptions.SSLError as e:
        logger.debug(e)
        if url.startswith('https://www.'):
            if verify:
                logger.warning('Encountered SSL error, trying without SSL certificate verification')
                download_page(url, filename, directory, verify=False)
            else:
                logger.error('Encountered SSL error even without verification, skipping')
        else:
            logger.warning('Encountered SSL error, trying with added "www."')
            download_page(add_www(url), filename, directory)
        return
    except requests.exceptions.ConnectionError as e:
        logger.debug(e)
        if url.startswith('https://www.'):
            logger.error('Encountered connection error, skipping')
        else:
            logger.warning('Encountered connection error, trying with added "www."')
            download_page(add_www(url), filename, directory)
        return
    except requests.exceptions.ReadTimeout as e:
        logger.debug(e)
        logger.error('Timed out on reading page, skipping')
        return
    except requests.exceptions.TooManyRedirects as e:
        logger.debug(e)
        logger.error('Encountered too many redirects, skipping')
        return

    data = response.content

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
