/**
 * Copyright 2016, RadiantBlue Technologies, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package access.messaging;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.PostConstruct;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

import messaging.job.JobMessageFactory;
import messaging.job.KafkaClientFactory;
import messaging.job.WorkerCallback;
import model.job.type.AbortJob;
import model.job.type.AccessJob;
import model.logger.AuditElement;
import model.logger.Severity;
import model.request.PiazzaJobRequest;
import util.PiazzaLogger;

/**
 * Manager class for Access Jobs. Handles an incoming Access Job request by creating a GeoServer deployment, or
 * returning a file location. This manages the Thread Pool of Access Workers.
 * 
 * @author Patrick.Doody
 * 
 */
@Component
public class AccessThreadManager {

	@Autowired
	private PiazzaLogger pzLogger;
	@Autowired
	private AccessWorker accessWorker;
	@Autowired
	private ObjectMapper objectMapper;

	@Value("${vcap.services.pz-kafka.credentials.host}")
	private String kafkaAddress;

	private String kafkaHost;

	private String kafkaPort;

	@Value("#{'${kafka.group}' + '-' + '${SPACE}'}")
	private String kafkaGroup;

	@Value("${SPACE}")
	private String space;

	private Producer<String, String> producer;
	private Map<String, Future<?>> runningJobs;
	private final AtomicBoolean closed = new AtomicBoolean(false);

	private static final Logger LOGGER = LoggerFactory.getLogger(AccessThreadManager.class);
	private static final String LOGGER_FORMAT = "%s-%s";
	private static final String ACCESS_TOPIC_NAME = AccessJob.class.getSimpleName();

	/**
	 * Manages the Access Jobs Thread Pools
	 */
	public AccessThreadManager() {
		// Empty Constructor for unit tests
	}

	/**
	 * Initializes consumer listeners for the thread pool workers.
	 */
	@PostConstruct
	public void initialize() {
		// Initialize the Kafka Producer
		kafkaHost = kafkaAddress.split(":")[0];
		kafkaPort = kafkaAddress.split(":")[1];
		producer = KafkaClientFactory.getProducer(kafkaHost, kafkaPort);

		// Initialize the Map of running Threads
		runningJobs = new HashMap<>();

		// Start polling for Kafka Jobs on the Group Consumer.
		// Occurs on a separate Thread to not block Spring.
		new Thread(() -> pollAccessJobs()).start();

		// Start polling for Kafka Abort Jobs on the unique Consumer.
		new Thread(() -> pollAbortJobs()).start();
	}

	/**
	 * Opens up a Kafka Consumer to poll for all Access Jobs that should be processed by this component.
	 */
	public void pollAccessJobs() {
		try {
			// Callback that will be invoked when a Worker completes. This will
			// remove the Job Id from the running Jobs list.
			WorkerCallback callback = jobId -> runningJobs.remove(jobId);

			// Create the General Group Consumer
			Consumer<String, String> generalConsumer = KafkaClientFactory.getConsumer(kafkaHost, kafkaPort, kafkaGroup);
			generalConsumer.subscribe(Arrays.asList(String.format(LOGGER_FORMAT, ACCESS_TOPIC_NAME, space)));

			// Poll
			while (!closed.get()) {
				ConsumerRecords<String, String> consumerRecords = generalConsumer.poll(1000);
				// Handle new Messages on this topic.
				for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
					pzLogger.log(String.format("Processing Job ID %s on Access Worker Thread.", consumerRecord.key()), Severity.INFORMATIONAL);
					// Create a new worker to process this message and add it to
					// the thread pool.
					Future<?> workerFuture = accessWorker.run(consumerRecord, producer, callback);

					// Keep track of all Running Jobs
					runningJobs.put(consumerRecord.key(), workerFuture);
				}
			}
			generalConsumer.close();
		} catch (WakeupException exception) {
			String error = String.format("Polling Thread forcefully closed: %s", exception.getMessage());
			LOGGER.error(error, exception, new AuditElement("access", "kafkaListenerShutDown", ""));
			pzLogger.log(error, Severity.ERROR);
		}
	}

	/**
	 * Begins listening for Abort Jobs. If a Job is owned by this component, then it will be terminated.
	 */
	public void pollAbortJobs() {
		try {
			// Create the Unique Consumer
			Consumer<String, String> uniqueConsumer = KafkaClientFactory.getConsumer(kafkaHost, kafkaPort,
					String.format(LOGGER_FORMAT, kafkaGroup, UUID.randomUUID().toString()));
			uniqueConsumer.subscribe(Arrays.asList(String.format(LOGGER_FORMAT, JobMessageFactory.ABORT_JOB_TOPIC_NAME, space)));

			// Poll
			while (!closed.get()) {
				ConsumerRecords<String, String> consumerRecords = uniqueConsumer.poll(1000);
				// Handle new Messages on this topic.
				for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
					handleNewMessages(consumerRecord);
				}
			}
			uniqueConsumer.close();
		} catch (WakeupException exception) {
			String error = String.format("Polling Thread forcefully closed: %s", exception.getMessage());
			LOGGER.error(error, exception);
			pzLogger.log(error, Severity.ERROR);
		}
	}

	private void handleNewMessages(ConsumerRecord<String, String> consumerRecord) {
		// Determine if this Job Id is being processed by this
		// component.
		String jobId = getJobId(consumerRecord.value());
		if (jobId == null) {
			return;
		}

		if (runningJobs.containsKey(jobId)) {
			// Cancel the Running Job
			runningJobs.get(jobId).cancel(true);
			// Remove it from the list of Running Jobs
			runningJobs.remove(jobId);
		}
	}

	/**
	 * Stops all polling
	 */
	public void stopPolling() {
		this.closed.set(true);
	}

	/**
	 * Returns a list of the Job Ids that are currently being processed by this instance
	 * 
	 * @return The list of Job Ids
	 */
	public List<String> getRunningJobIds() {
		return new ArrayList<>(runningJobs.keySet());
	}

	private String getJobId(String consumerRecordValue) {
		try {
			PiazzaJobRequest request = objectMapper.readValue(consumerRecordValue, PiazzaJobRequest.class);
			return ((AbortJob) request.jobType).getJobId();
		} catch (Exception exception) {
			String error = String.format("Error Aborting Job. Could not get the Job ID from the Kafka Message with error:  %s",
					exception.getMessage());
			LOGGER.error(error, exception);
			pzLogger.log(error, Severity.ERROR);
			return null;
		}
	}
}
