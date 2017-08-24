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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.Exchange;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.QueueBinding;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

import messaging.job.JobMessageFactory;
import messaging.job.WorkerCallback;
import model.job.Job;
import model.job.type.AbortJob;
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

	@Value("${SPACE}")
	private String space;

	private Map<String, Future<?>> runningJobs = new HashMap<>();
	private static final Logger LOGGER = LoggerFactory.getLogger(AccessThreadManager.class);

	/**
	 * Processes an Abort Job message coming in through the queue. If this component contains that job, it will cancel
	 * it.
	 * 
	 * @param abortJobRequest
	 *            The request containing the information about the Job cancellation request
	 */
	@RabbitListener(bindings = @QueueBinding(key = "AbortJob-${SPACE}", value = @Queue(value = "AccessAbort", autoDelete = "true", durable = "true"), exchange = @Exchange(value = JobMessageFactory.PIAZZA_EXCHANGE_NAME, autoDelete = "false", durable = "true")))
	public void processAbortJob(final String abortJobRequest) {
		String jobId = null;
		try {
			PiazzaJobRequest request = objectMapper.readValue(abortJobRequest, PiazzaJobRequest.class);
			jobId = ((AbortJob) request.jobType).getJobId();
		} catch (Exception exception) {
			String error = String.format("Error Aborting Job. Could not get the Job ID from the Message with error:  %s",
					exception.getMessage());
			LOGGER.error(error, exception);
			pzLogger.log(error, Severity.ERROR);
		}

		if (runningJobs.containsKey(jobId)) {
			// Cancel the Running Job
			runningJobs.get(jobId).cancel(true);
			// Remove it from the list of Running Jobs
			runningJobs.remove(jobId);
		}
	}

	/**
	 * Processes an Access Job message coming through the Queue
	 * 
	 * @param accessJobRequest
	 *            The Access Job request
	 */
	@RabbitListener(bindings = @QueueBinding(key = "AccessJob-${SPACE}", value = @Queue(value = "AccessJob", autoDelete = "true", durable = "true"), exchange = @Exchange(value = JobMessageFactory.PIAZZA_EXCHANGE_NAME, autoDelete = "false", durable = "true")))
	public void processAccessJob(String accessJobRequest) {
		try {
			// Callback that will be invoked when a Worker completes. This will
			// remove the Job Id from the running Jobs list.
			WorkerCallback callback = (String jobId) -> runningJobs.remove(jobId);
			// Get the Job Model
			Job job = objectMapper.readValue(accessJobRequest, Job.class);
			// Process the work
			Future<?> workerFuture = accessWorker.run(job, callback);
			// Keep track of this running Job's ID
			runningJobs.put(job.getJobId(), workerFuture);
		} catch (IOException exception) {
			String error = String.format("Error Reading Access Job Message from Queue %s", exception.getMessage());
			LOGGER.error(error, exception);
			pzLogger.log(error, Severity.ERROR);
		}
	}

	/**
	 * Returns a list of the Job Ids that are currently being processed by this instance
	 * 
	 * @return The list of Job Ids
	 */
	public List<String> getRunningJobIds() {
		return new ArrayList<>(runningJobs.keySet());
	}

}
