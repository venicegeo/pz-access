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

import messaging.job.JobMessageFactory;
import messaging.job.WorkerCallback;
import model.data.DataResource;
import model.data.deployment.Deployment;
import model.job.Job;
import model.job.result.type.DeploymentResult;
import model.job.result.type.ErrorResult;
import model.job.result.type.FileResult;
import model.job.type.AccessJob;
import model.status.StatusUpdate;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;

import util.PiazzaLogger;
import access.database.MongoAccessor;
import access.deploy.Deployer;
import access.deploy.Leaser;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Worker class that handles Access Jobs being passed in through the Dispatcher.
 * Handles the Access Jobs by standing up services, retrieving files, or other
 * various forms of Access for data.
 * 
 * This component assumes that the data intended to be accessed is already
 * ingested into the Piazza system; either by the Ingest component or other
 * components that are capable of inserting data into Piazza.
 * 
 * @author Patrick.Doody
 * 
 */
public class AccessWorker implements Runnable {
	private ConsumerRecord<String, String> consumerRecord;
	private PiazzaLogger logger;
	private Producer<String, String> producer;
	private WorkerCallback callback;
	private Deployer deployer;
	private Leaser leaser;
	private MongoAccessor accessor;

	/**
	 * Creates an Access Worker instance.
	 * 
	 * @param consumerRecord
	 *            The Kafka message containing the Access Job
	 * @param producer
	 *            The Kafka Producer, used for sending Update messages
	 * @param accessor
	 *            the Mongo DB Accessor
	 * @param deployer
	 *            Deployer class that handles the GeoServer deployment
	 * @param leaser
	 *            Leaser class that generates Leases for Deployments
	 * @param workerCallback
	 *            The callback to be invoked upon completion of this worker
	 * @param logger
	 *            The Piazza Logger for logging
	 */
	public AccessWorker(ConsumerRecord<String, String> consumerRecord, Producer<String, String> producer,
			MongoAccessor accessor, Deployer deployer, Leaser leaser, WorkerCallback callback, PiazzaLogger logger) {
		this.consumerRecord = consumerRecord;
		this.producer = producer;
		this.accessor = accessor;
		this.deployer = deployer;
		this.leaser = leaser;
		this.callback = callback;
		this.logger = logger;
	}

	/**
	 * Listens for Kafka Access messages for creating Deployments for Access of
	 * Resources
	 */
	public void run() {
		try {
			// Parse Job information from Kafka
			ObjectMapper mapper = new ObjectMapper();
			Job job = mapper.readValue(consumerRecord.value(), Job.class);
			AccessJob accessJob = (AccessJob) job.jobType;

			// Logging
			logger.log(String.format("Received Request to Access Data %s of Type %s under Job ID",
					accessJob.getDataId(), accessJob.getDeploymentType(), job.getJobId()), PiazzaLogger.INFO);

			// Update Status that this Job is being processed
			StatusUpdate statusUpdate = new StatusUpdate(StatusUpdate.STATUS_RUNNING);
			producer.send(JobMessageFactory.getUpdateStatusMessage(consumerRecord.key(), statusUpdate));

			// Depending on how the user wants to Access the
			// Resource
			switch (accessJob.getDeploymentType()) {
			case AccessJob.ACCESS_TYPE_FILE:
				// Return the URL that the user can use to acquire the file
				statusUpdate = new StatusUpdate(StatusUpdate.STATUS_SUCCESS);
				statusUpdate.setResult(new FileResult(accessJob.getDataId()));
				producer.send(JobMessageFactory.getUpdateStatusMessage(consumerRecord.key(), statusUpdate));

				// Console Logging
				logger.log(String.format("GeoServer File Request successul for Resource %s", accessJob.getDataId()),
						PiazzaLogger.INFO);
				System.out.println("Deployment File Request Returned for Resource " + accessJob.getDataId());
				break;
			case AccessJob.ACCESS_TYPE_GEOSERVER:
				Deployment deployment = null;
				// Check if a Deployment already exists
				boolean exists = deployer.doesDeploymentExist(accessJob.getDataId());
				if (exists) {
					System.out.println("Renewing Deployment Lease for " + accessJob.getDataId());
					// If it does, then renew the Lease on the
					// existing deployment.
					deployment = accessor.getDeploymentByDataId(accessJob.getDataId());
					leaser.renewDeploymentLease(deployment);
				} else {
					System.out.println("Creating a new Deployment and lease for " + accessJob.getDataId());
					// Obtain the Data to be deployed
					DataResource dataToDeploy = accessor.getData(accessJob.getDataId());
					// Create the Deployment
					deployment = deployer.createDeployment(dataToDeploy);
					// Create a new Lease for this Deployment
					leaser.createDeploymentLease(deployment);
				}
				// Update Job Status to complete for this Job.
				statusUpdate = new StatusUpdate(StatusUpdate.STATUS_SUCCESS);
				statusUpdate.setResult(new DeploymentResult(deployment));
				producer.send(JobMessageFactory.getUpdateStatusMessage(consumerRecord.key(), statusUpdate));

				// Console Logging
				logger.log(String.format("GeoServer Deployment successul for Resource %s", accessJob.getDataId()),
						PiazzaLogger.INFO);
				System.out.println("Deployment Successfully Returned for Resource " + accessJob.getDataId());
				break;
			default:
				throw new Exception("Unknown Deployment Type: " + accessJob.getDeploymentType());
			}
		} catch (Exception exception) {
			logger.log(
					String.format("Error Accessing Data under Job %s with Error: %s", consumerRecord.key(),
							exception.getMessage()), PiazzaLogger.ERROR);
			exception.printStackTrace();
			try {
				// Send the failure message to the Job Manager.
				StatusUpdate statusUpdate = new StatusUpdate(StatusUpdate.STATUS_ERROR);
				statusUpdate.setResult(new ErrorResult("Could not Deploy Data", exception.getMessage()));
				producer.send(JobMessageFactory.getUpdateStatusMessage(consumerRecord.key(), statusUpdate));
			} catch (JsonProcessingException jsonException) {
				// If the Kafka message fails to send, at least log
				// something in the console.
				System.out
						.println("Could update Job Manager with failure event in Ingest Worker. Error creating message: "
								+ jsonException.getMessage());
				jsonException.printStackTrace();
			}
		} finally {
			callback.onComplete(consumerRecord.key());
		}
	}
}
