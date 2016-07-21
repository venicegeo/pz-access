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
import java.util.List;
import java.util.concurrent.Future;

import messaging.job.JobMessageFactory;
import messaging.job.WorkerCallback;
import model.data.DataResource;
import model.data.deployment.Deployment;
import model.data.deployment.DeploymentGroup;
import model.job.Job;
import model.job.result.type.DeploymentResult;
import model.job.result.type.ErrorResult;
import model.job.type.AccessJob;
import model.status.StatusUpdate;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Component;

import util.PiazzaLogger;
import access.database.Accessor;
import access.deploy.Deployer;
import access.deploy.GroupDeployer;
import access.deploy.Leaser;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

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
	private PiazzaLogger logger;
	@Value("${SPACE}")
	private String SPACE;

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
			accessJob = (AccessJob) job.jobType;

			// Logging
			logger.log(String.format("Received Request to Access Data %s of Type %s under Job ID %s", accessJob.getDataId(),
					accessJob.getDeploymentType(), job.getJobId()), PiazzaLogger.INFO);

			// Update Status that this Job is being processed
			StatusUpdate statusUpdate = new StatusUpdate(StatusUpdate.STATUS_RUNNING);
			producer.send(JobMessageFactory.getUpdateStatusMessage(consumerRecord.key(), statusUpdate, SPACE));

			// Depending on how the user wants to Access the Resource
			switch (accessJob.getDeploymentType()) {
			case AccessJob.ACCESS_TYPE_GEOSERVER:
				Deployment deployment = null;

				// Check if a Deployment already exists
				boolean exists = deployer.doesDeploymentExist(accessJob.getDataId());
				if (exists) {
					System.out.println("Renewing Deployment Lease for " + accessJob.getDataId());
					// If it does, then renew the Lease on the
					// existing deployment.
					deployment = accessor.getDeploymentByDataId(accessJob.getDataId());
					leaser.renewDeploymentLease(deployment, accessJob.getDurationDays());
				} else {
					System.out.println("Creating a new Deployment and lease for " + accessJob.getDataId());
					// Obtain the Data to be deployed
					DataResource dataToDeploy = accessor.getData(accessJob.getDataId());
					// Create the Deployment
					deployment = deployer.createDeployment(dataToDeploy);
					// Create a new Lease for this Deployment
					leaser.createDeploymentLease(deployment, accessJob.getDurationDays());
				}

				// Check if the user has requested this layer be added to a new
				// group layer.
				if ((accessJob.getDeploymentGroupId() != null) && (accessJob.getDeploymentGroupId().isEmpty() == false)) {
					// Check if the Deployment Group exists
					DeploymentGroup deploymentGroup = accessor.getDeploymentGroupById(accessJob.getDeploymentGroupId());
					if (deploymentGroup == null) {
						throw new Exception(String.format("Deployment Group with ID %s does not exist.", accessJob.getDeploymentGroupId()));
					}
					// Add the Layer to the Deployment Group
					List<Deployment> deployments = new ArrayList<Deployment>();
					deployments.add(deployment);
					groupDeployer.updateDeploymentGroup(deploymentGroup, deployments);
				}

				// Update Job Status to complete for this Job.
				statusUpdate = new StatusUpdate(StatusUpdate.STATUS_SUCCESS);
				statusUpdate.setResult(new DeploymentResult(deployment));
				producer.send(JobMessageFactory.getUpdateStatusMessage(consumerRecord.key(), statusUpdate, SPACE));

				// Console Logging
				logger.log(String.format("GeoServer Deployment successul for Resource %s", accessJob.getDataId()), PiazzaLogger.INFO);
				System.out.println("Deployment Successfully Returned for Resource " + accessJob.getDataId());
				break;
			default:
				throw new Exception("Unknown Deployment Type: " + accessJob.getDeploymentType());
			}
		} catch (Exception exception) {
			logger.log(String.format("Error Accessing Data under Job %s with Error: %s", consumerRecord.key(), exception.getMessage()),
					PiazzaLogger.ERROR);
			exception.printStackTrace();
			try {
				// Send the failure message to the Job Manager.
				StatusUpdate statusUpdate = new StatusUpdate(StatusUpdate.STATUS_ERROR);
				statusUpdate.setResult(new ErrorResult("Could not Deploy Data", exception.getMessage()));
				producer.send(JobMessageFactory.getUpdateStatusMessage(consumerRecord.key(), statusUpdate, SPACE));
			} catch (JsonProcessingException jsonException) {
				// If the Kafka message fails to send, at least log
				// something in the console.
				System.out.println("Could not update Job Manager with failure event in Ingest Worker. Error creating message: "
						+ jsonException.getMessage());
				jsonException.printStackTrace();
			}
		} finally {
			if (callback != null) {
				callback.onComplete(consumerRecord.key());
			}
		}

		return new AsyncResult<AccessJob>(accessJob);
	}
}
