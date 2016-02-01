package access.messaging;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.PostConstruct;

import messaging.job.KafkaClientFactory;
import model.job.Job;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import access.deploy.Deployer;
import access.deploy.Leaser;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Worker class that listens for Access Jobs being passed in through the
 * Dispatcher. Handles the Access Jobs by standing up services, retrieving
 * files, or other various forms of Access for data.
 * 
 * This component assumes that the data intended to be accessed is already
 * ingested into the Piazza system; either by the Ingest component or other
 * components that are capable of inserting data into Piazza.
 * 
 * @author Patrick.Doody
 * 
 */
@Component
public class AccessWorker {
	private static final String ACCESS_TOPIC_NAME = "access";
	@Autowired
	private Deployer deployer;
	@Autowired
	private Leaser leaser;
	@Value("${kafka.host}")
	private String KAFKA_HOST;
	@Value("${kafka.port}")
	private String KAFKA_PORT;
	@Value("${kafka.group}")
	private String KAFKA_GROUP;
	private Producer<String, String> producer;
	private Consumer<String, String> consumer;
	private final AtomicBoolean closed = new AtomicBoolean(false);

	/**
	 * Worker class that listens for and processes Access messages.
	 */
	public AccessWorker() {
	}

	/**
	 * 
	 */
	@PostConstruct
	public void initialize() {
		// Initialize the Kafka consumer/producer
		producer = KafkaClientFactory.getProducer(KAFKA_HOST, KAFKA_PORT);
		consumer = KafkaClientFactory.getConsumer(KAFKA_HOST, KAFKA_PORT, KAFKA_GROUP);
		// Listen for events
		listen();
	}

	/**
	 * Listens for Kafka Access messages
	 */
	public void listen() {
		try {
			consumer.subscribe(Arrays.asList(ACCESS_TOPIC_NAME));
			while (!closed.get()) {
				ConsumerRecords<String, String> consumerRecords = consumer.poll(1000);
				// Handle new Messages on this topic.
				for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
					System.out.println("Processing Access Message " + consumerRecord.topic() + " with key "
							+ consumerRecord.key());
					// Wrap the JobRequest in the Job object
					try {
						ObjectMapper mapper = new ObjectMapper();
						Job job = mapper.readValue(consumerRecord.value(), Job.class);
					} catch (Exception exception) {
						System.out.println("An unexpected error occurred while processing the Job Message: "
								+ exception.getMessage());
						exception.printStackTrace();
					}
				}
			}
		} catch (WakeupException exception) {
			// Ignore exception if closing
			if (!closed.get()) {
				throw exception;
			}
		} finally {
			consumer.close();
		}
	}
}
