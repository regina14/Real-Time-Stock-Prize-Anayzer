# 
# - kafka broker
# - kafka topic
# - cassandra brokcer
# - cassandra keyspace/table

import json
import argparse
import logging
import datetime
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from cassandra.cluster import Cluster

topic_name = None
kafka_broker = None
key_space = None
data_table = None
contact_point = None

consumer = None

# set up logging 
logging.basicConfig()
logger = logging.getLogger('data_storage')
logger.setLevel(logging.DEBUG)
def covertTime(timestamp):
	return datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')

def persist_data(stock_data, cassandra_session):
	logging.debug('Begain to save data %s', stock_data)
	parsed = json.loads(stock_data)
	symbol = parsed.get('symbol')
	trade_time = covertTime(parsed.get('regularMarketTime'))
	price = float(parsed.get('regularMarketPrice').get('raw'))
	logging.debug("stock symbol is %s, price is %f" % (symbol, price))
	#statement = "INSERT INTO %s (stock_symbol, trade_time, trade_price) VALUES ('%s', '%s', %f)" % (data_table, symbol, tradetime, price)
	statement = "INSERT INTO %s (stock_symbol, trade_time, trade_price) VALUES ('%s', '%s', %f)" % (data_table, symbol, trade_time, price)
	cassandra_session.execute(statement);
	logger.info("Persist data to cassandra for symbol %s, price %f, tradetime %s" % (symbol, price, trade_time))
	

if __name__ == '__main__':
	# set up command line for arguments
	parser = argparse.ArgumentParser()
	parser.add_argument('topic_name', help = 'the kafka topic to subscribe from')
	parser.add_argument('kafka_broker', help = 'the location of the kafka broker')
	parser.add_argument('key_space', help = 'the keyspace for cassandra')
	parser.add_argument('data_table', help = 'the table will be used')
	parser.add_argument('contact_point', help = 'the contact_point for cassandra')

	# - parse arguments
	args = parser.parse_args()
	topic_name = args.topic_name
	kafka_broker = args.kafka_broker
	key_space = args.key_space
	data_table = args.data_table
	contact_point = args.contact_point

	# - create kafka consumer
	# bootstrap is like a contact point at the begining of connection
	consumer = KafkaConsumer(
		topic_name, 
		bootstrap_servers = kafka_broker
	)

	# create a cassandra session
	cassandra_cluster = Cluster(
		contact_points = contact_point.split(',')
	)

	session = cassandra_cluster.connect()
	session.execute("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class':'SimpleStrategy', 'replication_factor': '3'} AND durable_writes = 'true'" % key_space)
					#"CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class':'SimpleStrategy', 'replication_factor': '3'} AND durable_writes = 'true'" % key_space
	session.set_keyspace(key_space)
	session.execute("CREATE TABLE IF NOT EXISTS %s (stock_symbol text, trade_time timestamp, trade_price float, PRIMARY KEY (stock_symbol, trade_time))" % data_table)
					#"CREATE TABLE IF NOT EXISTS %s (stock_symbol text, trade_time timestamp, trade_price float, PRIMARY KEY (stock_symbol, trade_time))" % data_table

	for msg in consumer:
		persist_data(msg.value, session)