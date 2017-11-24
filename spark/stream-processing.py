# - read data from kafka
# - process
# - send data back to kafka

# - kafka location
# - kafka topic

import argparse
import logging
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

logging.basicConfig()
logger = logging.getLogger('stream-processing')
logger.setLevel(logging.INFO)


def process(timeobject, rdd):
	# - 
	logger.info(rdd)

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

	# - set up streaming utils
	sc = SparkContext("local[2]", "StockAveragePrice")
	ssc = StreamingContext(sc, 5)

	# - 
	kafkaStream = KafkaUtils.createDirectStream(ssc, [topic], {'metadata.broker.list': kafka_broker})
	kafkaStream.foreachRDD(process)

	ssc.start()
	ssc.awaitTermination()

