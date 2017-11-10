# - connect to any kafka broker, ip and broker port
# - grad stock price
# - for one stock, grab once a second
# - change which stock to grab dynamically
from apscheduler.schedulers.background import BackgroundScheduler
from kafka import KafkaProducer
import argparse
import json
import time
import requests
import logging


#from googlefinance import getQuotes
from kafka.errors import(
    KafkaError,
    KafkaTimeoutError
)

# log set up
logging.basicConfig()
logger= logging.getLogger('data-producer')

logger.setLevel(logging.DEBUG)

producer = None
topic_name = None

# schedular set up
schedular = BackgroundScheduler()
schedular.add_executor('threadpool')
schedular.start()

def fetch_price(symbol):
	#getQuotes(symbol)
	logger.debug('Start to fetch price for %s.' % symbol)
	rsp = requests.get('https://finance.google.com/finance?q=' + symbol +'&output=json')
	data = json.loads(rsp.content[6:-2].decode('unicode_escape'))
	d = json.dumps(data)
	logger.debug('Recieve stock price %s.' % d)
	producer.send(topic = topic_name, value = d, timestamp_ms = time.time())
	logger.debug('Send stock price for %s' % symbol)

if __name__ == '__main__':
	# - parse user command line argeument
	parser = argparse.ArgumentParser()
	parser.add_argument('kafka_broker', help = 'the location of kafka broker')
	parser.add_argument('topic_name', help = 'the kafka topic to write to')
	args = parser.parse_args()
	kafka_broker = args.kafka_broker
	topic_name = args.topic_name
	producer = KafkaProducer(bootstrap_servers= kafka_broker)
	schedular.add_job(fetch_price, 'interval', ['AAPL'], seconds = 1, id = 'AAPL')

	while True:
		pass

