import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class Application {
	private static final String TOPIC = "events";
	// If one server fails, producer still has two servers to connect to cluster
	// private static final String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094";
	private static final String BOOTSTRAP_SERVERS = "localhost:9092";
	public static void main(String [] args) {
		Producer<Long, String> kafkaProducer = createKafkaProducer(BOOTSTRAP_SERVERS);
		try {
			produceMessages(1000, kafkaProducer);
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}finally {
			kafkaProducer.flush();
			kafkaProducer.close();
		}
		
	}
	
	
	public static void produceMessages(int numberOfMessages, Producer<Long, String> kafkaProducer) throws InterruptedException, ExecutionException {
		//int partition = 0;
		
		for (int i = 0; i < numberOfMessages; i++) {
			long key = i;
			String value = String.format("event %d", i);
			long timeStamp = System.currentTimeMillis();
			// Use key to determine the partition can be better so that it does not need to know how many partitions
			// if not using key, round robin choosing partition
			// The data for same key goes to same partition since Kafka uses a consistent hashing algorithm to map key to partitions.
			ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(TOPIC, key, value);
			// After send record to Kafka, we can get a record metadata object to tell where the record landed in the distributed kafka topic
			RecordMetadata recordMetaData = kafkaProducer.send(record).get();
			
			System.out.println(String.format("Record with (key: %s, value: %s), was sent to (partition: %d, offset: %d)", 
					record.key(), record.value(), recordMetaData.partition(), recordMetaData.offset()));
			Thread.sleep(500);
		}
	}
	public static Producer<Long, String> createKafkaProducer(String bootstrapServers){
		// configure the properties of Kafka producer
		// Key-value container passed to kafka producer
		Properties properties = new Properties();
		
		// Comma separated list of Kafka server addresses producer will initially use to establish connection to the kafka cluster 
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		
		// Client Id: a human readable name for logging in purpose
		properties.put(ProducerConfig.CLIENT_ID_CONFIG, "events-producer");
		
		// Kafka use any java object as a key, java object type as a value
		// Each key-value pair, we need to tell kafka library how to serialize the object to binary format so it can be sent by network.
		
		// Kafka producer: key: long value: String
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		// Create a kafka producer object and pass the properties into the constructor
		return new KafkaProducer<Long, String>(properties);
	}
}
