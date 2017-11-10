# Real-Time-Stock-Prize-Anayzer

The project is mainly about developing a real-time stock analyzer using popular big data engineering technology stack. 

The whole project is divided into four layers, including data ingestion, storage, computation and visualization layers. 

For the ingestion layer, raw data was caught by google finance API into Kafka and zookeeper cluster. 

For the second layer, data is transferred and stored in Cassandra database. 

For the data processing layer,  I used Spark to calculate the real-time average price for a stock and maybe try to do a simple prediction about the future price. 

Finally, I built a dashboard to display the analysis result. 
