# - read data from kafka
# - process
# - send data back to kafka

# - kafka location
# - kafka topic

import argparse
import logging
import json
import time
import atexit
from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

logging.basicConfig()
logger = logging.getLogger('stream-processing')
logger.setLevel(logging.INFO)
kafka_producer = None
kafka_broker = None
new_topic = None
topic = None

def shutdown_hook(producer):
	logger.info('Prepare to shutdown hook...')
	producer.flush(10)
	producer.close(10)

def process(stream):
	def send_to_kafka(rdd):
		results = rdd.collect()
		for r in results:
			data = json.dumps(
				{
				'symbol': r[0],
				'timestamp': time.time(),
				'average': r[1]
				}
			)
			try:
				logger.info('Sending average price %s to kafka' % data)
				kafka_producer.send(new_topic, value=data)
			except KafkaError as error:
				logger.warn('Failed to send average stock price to kafka, caused by: %s', error.message)

	def pair(data):
		record = json.loads(data[1].decode('utf-8'))
		#print record
		return record.get('symbol'), (float(record.get('regularMarketPrice').get('raw')), 1)
	stream.map(pair).reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])).map(lambda (k, v): (k, v[0]/v[1])).foreachRDD(send_to_kafka)


if __name__ == '__main__':
	# - setup command line arguments

	parser = argparse.ArgumentParser()
	parser.add_argument('kafka_broker', help = 'location of kafka')
	parser.add_argument('topic', help = 'original topic name')
	parser.add_argument('new_topic', help = 'new topic to send data to')

	# - get argumetns
	args = parser.parse_args()
	kafka_broker = args.kafka_broker
	topic = args.topic
	new_topic = args.new_topic

	kafka_producer = KafkaProducer(bootstrap_servers = kafka_broker)
	# - set up streaming utils
	sc = SparkContext("local[2]", "StockAveragePrice")
	sc.setLogLevel('ERROR')
	ssc = StreamingContext(sc, 5)

	# - 
	kafkaStream = KafkaUtils.createDirectStream(ssc, [topic], {'metadata.broker.list': kafka_broker})
	process(kafkaStream)
	atexit.register(shutdown_hook,kafka_producer)
	ssc.start()
	ssc.awaitTermination()

