Project Run Procedure
1. Start Kafka server as per given Kafka Installation guide
2. Create a Topic name in Kafka, make sure this topic is same in Java project resource
config also.
3. Import all three Java projects in IDE as a maven project
4. Open SparkKafkaProducer -> AppProducer.java. Change data source (i.e., weather data)
file path.
5. Run SparkKafkaProducer and SparkKafkaConsumer Main Class.
6. Make sure that both producer and consumer are using the same topic. Topic name can be
changed in Project ->resource-> config.properties
7. Run the pig file to export data from HBase to text
8. Run SparkSQL project to create table and load data in Hive.
9. Write desired query in java to perform query for desiring data in Hive.
10. Finally, connect the Hive Table with Tableau to analyze data.