from kafka import KafkaConsumer

consumer = KafkaConsumer('test',bootstrap_servers = '192.168.99.100:9092')
for msg in consumer:
    print(msg)

print("jd")
