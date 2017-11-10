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

from flask import(
	Flask,
	request,
	jsonify
)

# log set up
logging.basicConfig()
logger= logging.getLogger('data-producer')
logger.setLevel(logging.DEBUG)

## build a web appliction
app = Flask(__name__)
app.config.from_envvar('ENV_CONFIG_FILE')
kafka_broker = app.config['CONFIG_KAFKA_ENDPOINT']
topic_name = app.config['CONFIG_KAFKA_TOPIC']
symbols = set()

producer = KafkaProducer(bootstrap_servers= kafka_broker)

# schedular set up
schedular = BackgroundScheduler()
schedular.add_executor('threadpool')
schedular.start()





@app.route('/<symbol>', methods=['POST'])
# get route
def add_stock(symbol):
	if not symbol:
		return jsonify({
			'error: ':'Stock symbol is not valid.'
			}), 400
	if symbol in symbols:
		pass
	else:
		symbol = symbol.encode('utf-8')
		symbols.add(symbol)
		logging.info('Add stock fetch job for %s' % symbol)
		schedular.add_job(fetch_price, 'interval', [symbol], seconds = 1, id = symbol)

	return jsonify(result = list(symbols)), 200

@app.route('/<symbol>', methods=['DELETE'])
def del_stock(symbol):
	if not symbol:
		return jsonify({
			'error: ':'Stock symbol is not valid.'
			}), 400
	if symbol not in symbols:
		pass
	else: 
		symbol = symbol.encode('utf-8')
		symbols.remove(symbol)
		logging.info('Remove stock fetch job for %s' % symbol)
		schedular.remove_job(symbol)

	return jsonify(result = list(symbols)), 200

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
	#parser = argparse.ArgumentParser()
	#parser.add_argument('kafka_broker', help = 'the location of kafka broker')
	#parser.add_argument('topic_name', help = 'the kafka topic to write to')
	#args = parser.parse_args()
	#kafka_broker = args.kafka_broker
	#topic_name = args.topic_name
	
	app.run(host = '0.0.0.0', port = app.config['CONFIG_APPLICATION_PORT'])