package cs523.SparkKafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class WeatherKafkaConsumer {

	private KafkaConsumer<String, String> consumer;
	private MyResources resources;
	private String lastKey = "";

	public WeatherKafkaConsumer(MyResources myResources) {
		this.resources = myResources;
		Properties props = new Properties();
		props.put("auto.offset.reset.policy", "latest");
		props.put("bootstrap.servers", "localhost:" + resources.PORT);
		props.put("group.id", "group01");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer",
				"org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer",
				"org.apache.kafka.common.serialization.StringDeserializer");
		this.consumer = new KafkaConsumer<String, String>(props);
		this.consumer.subscribe(Arrays.asList(resources.TOPIC));

		System.out.println("TOPIC -  " + resources.TOPIC);
	}

	public void Wait(MyPredict predictable, MyConsumerCallback callback) {
		while (predictable.check()) {

			ConsumerRecords<String, String> records = this.consumer.poll(Duration.ofMillis(100));
			for (ConsumerRecord<String, String> record : records) {
				if (record.key() == null || record.value() == null) {
					System.out
							.println("Record Got Corrapted- Key or Value is Null");

				} else {
					try {
						callback.consume(record.key(), record.value(),
								this.lastKey.compareTo(record.key()
										.toLowerCase()) != 0);
						this.lastKey = record.key().toLowerCase();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		}
	}

	@FunctionalInterface
	public interface MyPredict {
		boolean check();
	}

	@FunctionalInterface
	public interface MyConsumerCallback {
		void consume(String key, String val, boolean new_key);
	}

}
