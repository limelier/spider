# Spider

This is a master-workers ensemble for downloading the top 50 websites in every
country, according to [Alexa Top Sites](https://www.alexa.com/topsites).

## Description
The ensemble consists of two scripts: `master.py` and `worker.py`. 

The master script scrapes the Alexa Top Sites page when run, getting a list of 
all the supported countries (and using them to create the directory for each) and 
then getting the top 50 websites in every country (and creating a task for each).
It then pushes each task onto the message queue as a JSON, for the workers to
consume.

The worker script connects to the message broker and consumes each task, downloading
the required webpage to the necessary directory. 

## Usage
Start up an instance of RabbitMQ, editing `config.py` with its host if is not on
`localhost`.

Run `master.py` where you want to download the files - it will automatically create
directories for each country in the working directory.

Run as many instances of `worker.py` as needed. These programs will receive their
tasks from the message queue, downloading the websites into the appropriate
directory with the filename format of `<website_hostname>.html`. When they are done
with all the tasks, shut them down with `CTRL+C` (or another way to send a
`SIGINT`).

## Configuration
The configuration file (`config.py`) contains the following variables:
- broker: these settings concern the message broker (the RabbitMQ instance)
    - host: the host of the broker
      (default: `localhost`)
    - queue: the name of the queue which will be created for the tasks
      (default: `tasks`)
- logging: these settings concern the level of logging used by the application
    - base_level: the log level used for the imported libraries (default: `FATAL`)
    - script_level: the log level used for the scripts themselves (default: `INFO`)