package fr.erdprt.samples.kafka.simple.main;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.rule.KafkaEmbedded;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.junit4.SpringRunner;

import fr.erdprt.samples.kafka.simple.main.Receiver;
import fr.erdprt.samples.kafka.simple.main.Sender;

@RunWith(SpringRunner.class)
@SpringBootTest
public class SpringKafkaApplicationTest {
	
	private static final Logger LOGGER	=	LoggerFactory.getLogger(SpringKafkaApplicationTest.class);

	private static String HELLOWORLD_TOPIC = "helloworld.t";

	@Autowired
	private Sender sender;

	@Autowired
	private Receiver receiver;

	@Autowired
	private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

	@ClassRule
	public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, HELLOWORLD_TOPIC);

	@Before
	public void setUp() throws Exception {
		// wait until the partitions are assigned
		/*
		for (MessageListenerContainer messageListenerContainer : kafkaListenerEndpointRegistry
				.getListenerContainers()) {
			ContainerTestUtils.waitForAssignment(messageListenerContainer, embeddedKafka.getPartitionsPerTopic());
		}
		*/
	}

	@Ignore
	@Test
	public void testReceive() throws Exception {
		LOGGER.debug("testReceive");
		sender.send(HELLOWORLD_TOPIC, "Hello Spring Kafka!");

		receiver.getLatch().await(10000, TimeUnit.MILLISECONDS);
		assertThat(receiver.getLatch().getCount()).isEqualTo(0);
	}
	
	@Test
	public void testReceiveMultiple() throws Exception {
		LOGGER.debug("testReceive");
		
		for (int index=0;index<10;index++) {
			sender.send(HELLOWORLD_TOPIC, "Hello Spring Kafka! for index=" + index);
		}

	}
	
}