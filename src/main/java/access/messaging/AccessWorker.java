/**
f_ * Copyright 2016, RadiantBlue Technologies, Inc.
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
import java.util.List;
import java.util.concurrent.Future;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.MongoInterruptedException;

import access.database.Accessor;
import access.deploy.Deployer;
import access.deploy.GroupDeployer;
import access.deploy.Leaser;
import exception.DataInspectException;
import exception.GeoServerException;
import exception.InvalidInputException;
import messaging.job.JobMessageFactory;
import messaging.job.WorkerCallback;
import model.data.DataResource;
import model.data.deployment.Deployment;
import model.data.deployment.DeploymentGroup;
import model.job.Job;
import model.job.result.type.DeploymentResult;
import model.job.result.type.ErrorResult;
import model.job.type.AccessJob;
import model.logger.AuditElement;
import model.logger.Severity;
import model.status.StatusUpdate;
import util.PiazzaLogger;

/**
 * Worker class that handles Access Jobs being passed in through the Kafka. Handles the Access Jobs by standing up
 * services, retrieving files, or other various forms of Access for data.
 * 
 * This component assumes that the data intended to be accessed is already ingested into the Piazza system; either by
 * the Ingest component or other components that are capable of inserting data into Piazza.
 * 
 * @author Patrick.Doody & Sonny.Saniev
 * 
 */
@Component
public class AccessWorker {
	@Autowired
	private Deployer deployer;
	@Autowired
	private GroupDeployer groupDeployer;
	@Autowired
	private Accessor accessor;
	@Autowired
	private Leaser leaser;
	@Autowired
	private PiazzaLogger pzLogger;
	@Value("${SPACE}")
	private String space;

	private static final Logger LOGGER = LoggerFactory.getLogger(AccessWorker.class);

	/**
	 * Listens for Kafka Access messages for creating Deployments for Access of Resources
	 */
	@Async
	public Future<AccessJob> run(ConsumerRecord<String, String> consumerRecord, Producer<String, String> producer,
			WorkerCallback callback) {
		AccessJob accessJob = null;
		try {
			// Parse Job information from Kafka
			ObjectMapper mapper = new ObjectMapper();
			Job job = mapper.readValue(consumerRecord.value(), Job.class);
			accessJob = (AccessJob) job.getJobType();

			// Validate inputs for the Kafka Message
			if ((accessJob.getDataId() == null) || (accessJob.getDataId().isEmpty())) {
				throw new InvalidInputException(String.format("An invalid or empty Data Id was specified: %s", accessJob.getDataId()));
			}

			if ((accessJob.getDeploymentType() == null) || (accessJob.getDeploymentType().isEmpty())) {
				throw new InvalidInputException(
						String.format("An invalid or empty Deployment Type was specified: %s", accessJob.getDataId()));
			}

			// Logging
			pzLogger.log(
					String.format("Received Request to Access Data %s of Type %s under Job Id %s by user %s", accessJob.getDataId(),
							accessJob.getDeploymentType(), job.getJobId(), job.getCreatedBy()),
					Severity.INFORMATIONAL, new AuditElement(job.getJobId(), "requestAccessData", accessJob.getDataId()));

			if (Thread.interrupted()) {
				throw new InterruptedException();
			}

			processGeoServerType(job, accessJob, producer, consumerRecord.key());
			
		} catch (MongoInterruptedException | InterruptedException exception) {
			String error = String.format("Thread interrupt received for Job %s", consumerRecord.key());
			LOGGER.error(error, exception, new AuditElement(consumerRecord.key(), "accessJobTerminated", ""));
			pzLogger.log(error, Severity.INFORMATIONAL);
			StatusUpdate statusUpdate = new StatusUpdate(StatusUpdate.STATUS_CANCELLED);
			try {
				producer.send(JobMessageFactory.getUpdateStatusMessage(consumerRecord.key(), statusUpdate, space));
			} catch (JsonProcessingException jsonException) {
				error = String.format(
						"Error sending Cancelled Status from Job %s: %s. The Job was cancelled, but its status will not be updated in the Job Manager.",
						consumerRecord.key(), jsonException.getMessage());
				LOGGER.error(error, jsonException);
				pzLogger.log(error, Severity.ERROR);
			}
		} catch (Exception exception) {
			String error = String.format("Error Accessing Data under Job %s with Error: %s", consumerRecord.key(), exception.getMessage());
			LOGGER.error(error, exception, new AuditElement(consumerRecord.key(), "failedAccessData", ""));
			pzLogger.log(error, Severity.ERROR);

			try {
				// Send the failure message to the Job Manager.
				StatusUpdate statusUpdate = new StatusUpdate(StatusUpdate.STATUS_ERROR);
				statusUpdate.setResult(new ErrorResult("Could not Deploy Data", exception.getMessage()));
				producer.send(JobMessageFactory.getUpdateStatusMessage(consumerRecord.key(), statusUpdate, space));
			} catch (JsonProcessingException jsonException) {
				// If the Kafka message fails to send, at least log
				// something in the console.
				String errorJson = String.format(
						"Could not update Job Manager with failure event in Ingest Worker. Error creating message: %s",
						jsonException.getMessage());
				LOGGER.error(errorJson, jsonException);
				pzLogger.log(errorJson, Severity.ERROR, new AuditElement(consumerRecord.key(), "failedAccessData", errorJson));
			}
		} finally {
			if (callback != null) {
				callback.onComplete(consumerRecord.key());
			}
		}

		return new AsyncResult<>(accessJob);
	}
	
	private void processGeoServerType(Job job, AccessJob accessJob, Producer<String, String> producer, String key) throws JsonProcessingException, InvalidInputException, InterruptedException, GeoServerException, DataInspectException  {
		// Update Status that this Job is being processed
		StatusUpdate statusUpdate = new StatusUpdate(StatusUpdate.STATUS_RUNNING);
		producer.send(JobMessageFactory.getUpdateStatusMessage(key, statusUpdate, space));

		// Depending on how the user wants to Access the Resource
		if (accessJob.getDeploymentType().equals(AccessJob.ACCESS_TYPE_GEOSERVER)) {
			Deployment deployment;

			// Check if a Deployment already exists
			boolean exists = deployer.doesDeploymentExist(accessJob.getDataId());
			if (exists) {
				LOGGER.info("Renewing Deployment Lease for " + accessJob.getDataId());
				// If it does, then renew the Lease on the
				// existing deployment.
				deployment = accessor.getDeploymentByDataId(accessJob.getDataId());
				leaser.renewDeploymentLease(deployment, accessJob.getDurationDays());
			} else {
				LOGGER.info("Creating a new Deployment and lease for " + accessJob.getDataId());
				// Obtain the Data to be deployed
				DataResource dataToDeploy = accessor.getData(accessJob.getDataId());
				if (dataToDeploy == null) {
					throw new InvalidInputException(String.format("Data with Id %s does not exist.", accessJob.getDataId()));
				}
				// Create the Deployment
				deployment = deployer.createDeployment(dataToDeploy);
				// Create a new Lease for this Deployment
				leaser.createDeploymentLease(deployment, accessJob.getDurationDays());
			}

			if (Thread.interrupted()) {
				throw new InterruptedException();
			}

			// Check if the user has requested this layer be added to a new group layer.
			if ((accessJob.getDeploymentGroupId() != null) && (!accessJob.getDeploymentGroupId().isEmpty())) {
				addToNewLayerGroup(deployment, accessJob);
			}

			if (Thread.interrupted()) {
				throw new InterruptedException();
			}

			// Update Job Status to complete for this Job.
			statusUpdate = new StatusUpdate(StatusUpdate.STATUS_SUCCESS);
			statusUpdate.setResult(new DeploymentResult(deployment));
			producer.send(JobMessageFactory.getUpdateStatusMessage(key, statusUpdate, space));

			// Console Logging
			pzLogger.log(String.format("GeoServer Deployment successful for Resource %s by user %s", accessJob.getDataId(), job.getCreatedBy()), Severity.INFORMATIONAL,
					new AuditElement(job.getJobId(), "accessData", accessJob.getDataId()));
			LOGGER.info("Deployment Successfully Returned for Resource " + accessJob.getDataId());
		} else {
			throw new InvalidInputException("Unknown Deployment Type: " + accessJob.getDeploymentType());
		}
	}
	
	private void addToNewLayerGroup(Deployment deployment, AccessJob accessJob) throws GeoServerException, InvalidInputException, DataInspectException  {
		// First verify that the Deployment exists in GeoServer . This is to avoid a race condition where
		// another Deployment Job in Piazza is responsible for creating the Deployment Layer for the Data ID
		// - but has not finished publishing this layer to GeoServer yet.
		boolean geoServerLayerExists = false;
		try {
			geoServerLayerExists = deployer.doesGeoServerLayerExist(deployment.getLayer());
		} catch (Exception exception) {
			String error = String.format("Could not create Deployment Group: %s", exception.getMessage());
			LOGGER.error(error, exception);
			pzLogger.log(error, Severity.ERROR);
			throw new GeoServerException(error);
		}

		if (geoServerLayerExists) {
			// First, Check if the Deployment Group exists
			DeploymentGroup deploymentGroup = accessor.getDeploymentGroupById(accessJob.getDeploymentGroupId());
			if (deploymentGroup == null) {
				throw new InvalidInputException(
						String.format("Deployment Group with Id %s does not exist.", accessJob.getDeploymentGroupId()));
			}
			
			// Add the Layer to the Deployment Group
			List<Deployment> deployments = new ArrayList<>();
			deployments.add(deployment);
			groupDeployer.updateDeploymentGroup(deploymentGroup, deployments);
		} else {
			// If the Layer does not exist on GeoServer yet, but Piazza has reported that a Deployment
			// exists, then another Job is likely processing this Deployment and has not yet finished. Send
			// an error back to the user to try again later.
			String error = String.format("Could not create Deployment Group. The GeoServer layer for %s does not exist.",
					deployment.getLayer());
			pzLogger.log(error, Severity.WARNING);
			throw new GeoServerException(error);
		}
	}
}
