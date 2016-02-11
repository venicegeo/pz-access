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

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.PostConstruct;

import messaging.job.JobMessageFactory;
import messaging.job.KafkaClientFactory;
import model.data.DataResource;
import model.job.Job;
import model.job.type.AccessJob;
import model.status.StatusUpdate;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import access.database.MongoAccessor;
import access.database.model.Deployment;
import access.deploy.Deployer;
import access.deploy.Leaser;

import com.fasterxml.jackson.core.JsonProcessingException;
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
	@Autowired
	private MongoAccessor accessor;
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
		// Listen for events TODO: Talk to Sonny about moving to @Async method
		Thread pollThread = new Thread() {
			public void run() {
				listen();
			}
		};
		pollThread.start();
	}

	/**
	 * Listens for Kafka Access messages for creating Deployments for Access of
	 * Resources
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
					try {
						ObjectMapper mapper = new ObjectMapper();
						Job job = mapper.readValue(consumerRecord.value(), Job.class);
						AccessJob accessJob = (AccessJob) job.jobType;

						// Update Status that this Job is being processed
						StatusUpdate statusUpdate = new StatusUpdate(StatusUpdate.STATUS_RUNNING);
						producer.send(JobMessageFactory.getUpdateStatusMessage(consumerRecord.key(), statusUpdate));

						// Depending on how the user wants to Access the
						// Resource
						switch (accessJob.getDeploymentType()) {
						case AccessJob.ACCESS_TYPE_FILE:
							// TODO: Return FTP link? S3 link? Stream the bytes?
							throw new Exception("File type not supported at this time.");
						case AccessJob.ACCESS_TYPE_GEOSERVER:
							// Check if a Deployment already exists
							boolean exists = deployer.doesDeploymentExist(accessJob.getDataId());
							if (exists) {
								// If it does, then renew the Lease on the
								// existing deployment.
								Deployment deployment = accessor.getDeploymentByResourceId(accessJob.getDataId());
								leaser.renewDeploymentLease(deployment);
							} else {
								// Obtain the Data to be deployed
								DataResource dataToDeploy = accessor.getData(accessJob.getDataId());
								// Create the Deployment
								Deployment deployment = deployer.createDeployment(dataToDeploy);
								// Create a new Lease for this Deployment
								leaser.createDeploymentLease(deployment);
							}
							break;
						default:
							throw new Exception("Unknown Deployment Type: " + accessJob.getDeploymentType());
						}

						// Update Job Status to complete for this Job.
						statusUpdate = new StatusUpdate(StatusUpdate.STATUS_SUCCESS);
						statusUpdate.setResult(null /* TODO: Set the result! */);
						producer.send(JobMessageFactory.getUpdateStatusMessage(consumerRecord.key(), statusUpdate));
					} catch (Exception exception) {
						// Handle any errors that occur.
						exception.printStackTrace();
						try {
							// Send the failure message to the Job Manager.
							producer.send(JobMessageFactory.getUpdateStatusMessage(consumerRecord.key(),
									new StatusUpdate(StatusUpdate.STATUS_ERROR)));
						} catch (JsonProcessingException jsonException) {
							// If the Kafka message fails to send, at least log
							// something in the console.
							System.out
									.println("Could update Job Manager with failure event in Ingest Worker. Error creating message: "
											+ jsonException.getMessage());
							jsonException.printStackTrace();
						}
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
