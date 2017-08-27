package fr.erdprt.samples.kafka;

import java.util.Properties;
import org.apache.kafka.clients.producer.*;

//import ProducerRecord packages
public class SimpleProducer {
	// Create java class named "SimpleProducer”
	public static void main(String[] args) throws Exception {
		if (args.length == 0)
		// Check arguments length value
		{
			System.out.println("Enter topic name");
			return;
		}
		String topicName = args[0].toString();
		// Assign topicName to string variable
		Properties props = new Properties();
		// create instance for properties to access producer configs
		props.put("bootstrap.servers", "localhost:9092");
		// Assign localhost id
		props.put("acks", "all");
		// Set acknowledgements for producer requests.
		props.put("retries", 0);
		// If the request fails, the producer can automatically retry,
		props.put("batch.size", 16384);
		// Specify buffer size in config
		props.put("linger.ms", 1);
		// Reduce the no of requests less than 0
		props.put("buffer.memory", 33554432);
		// The buffer.memory controls the total amount of memory available to
		// the producer for buffering.
		props.put("key.serializer", "org.apache.kafka.common.serializa-tion.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serializa-tion.StringSerializer");
		Producer<String, String> producer = new KafkaProducer<String, String>(props);
		for (int i = 0; i < 10; i++)
			producer.send(new ProducerRecord<String, String>(topicName, Integer.toString(i), Integer.toString(i)));
		System.out.println("Message sent successfully");
		producer.close();
	}
}