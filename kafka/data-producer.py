# - connect to any kafka broker, ip and broker port
# - grad stock price
# - for one stock, grab once a second
# - change which stock to grab dynamically
from apscheduler.schedulers.background import BackgroundScheduler
from kafka import KafkaProducer
import argparse
import json
import time
import atexit
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

# define shut down method to manage resource after the program terminats
def shutdown_hook():
	logger.debug('Prepare to exit')
	producer.flush(10)
	producer.close()
	logger.debug('Kafka producer closed')
	schedular.shutdown()
	logger.debug('Schedular shut down')



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
	rsp = requests.get('https://finance.yahoo.com/quote/' + symbol)
	r=rsp.text.encode('utf-8')
	i1=0
	i1=r.find('root.App.main', i1)
	i1=r.find('{', i1)
	i2=r.find("\n", i1)
	i2=r.rfind(';', i1, i2)
	jsonstr=r[i1:i2]   
	
	data = json.loads(jsonstr)['context']['dispatcher']['stores']['QuoteSummaryStore']['price']
	ss = data["symbol"].encode('utf-8')
	pp = data["regularMarketPrice"]["raw"]
	d = json.dumps(data, indent = 2)
	logger.debug('Recieve stock from %s price %f.' % (ss, pp))
	producer.send(topic = topic_name, value = d, timestamp_ms = time.time())
	logger.debug('Send stock price for %s\n' % symbol)

if __name__ == '__main__':
	atexit.register(shutdown_hook)
	app.run(host = '0.0.0.0', port = app.config['CONFIG_APPLICATION_PORT'])