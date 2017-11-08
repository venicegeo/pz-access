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
package access.test;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.core.RabbitTemplate;

import com.fasterxml.jackson.databind.ObjectMapper;

import access.database.DatabaseAccessor;
import access.deploy.Deployer;
import access.deploy.Leaser;
import access.messaging.AccessWorker;
import messaging.job.JobMessageFactory;
import messaging.job.WorkerCallback;
import model.job.Job;
import model.job.type.AccessJob;
import util.PiazzaLogger;

/**
 * Tests the Access Worker, which processes messages
 * 
 * @author Patrick.Doody
 *
 */
public class WorkerTests {
	@Mock
	private Deployer deployer;
	@Mock
	private DatabaseAccessor databaseAccessor;
	@Mock
	private Leaser leaser;
	@Mock
	private PiazzaLogger logger;
	@Mock
	private Queue updateJobsQueue;
	@Mock
	private RabbitTemplate rabbitTemplate;
	@Mock
	private ObjectMapper mapper;

	@InjectMocks
	private AccessWorker worker;

	/**
	 * Test initialization
	 */
	@Before
	public void setup() {
		MockitoAnnotations.initMocks(this);

	}

	/**
	 * Tests handling a message. Verify no exceptions.
	 */
	@Test
	public void testWorker() throws Exception {
		// Mock
		when(updateJobsQueue.getName()).thenReturn("Update-Job-Unit-Test");
		Mockito.doNothing().when(rabbitTemplate).convertAndSend(eq(JobMessageFactory.PIAZZA_EXCHANGE_NAME), eq("123456"),
				Mockito.anyString());
		Job mockJob = new Job();
		mockJob.setJobId("123456");
		mockJob.setCreatedBy("Test User");
		AccessJob accessJob = new AccessJob("123456");
		accessJob.deploymentType = AccessJob.ACCESS_TYPE_GEOSERVER;
		accessJob.dataId = "123456";
		mockJob.setJobType(accessJob);
		WorkerCallback callback = new WorkerCallback() {
			@Override
			public void onComplete(String jobId) {
				assertTrue(jobId.equals("123456"));
			}
		};

		// Test when refreshing an expired lease
		when(deployer.doesDeploymentExist(eq("123456"))).thenReturn(true);
		worker.run(mockJob, callback);

		// Test when a current lease doesn't exist - new is created
		when(deployer.doesDeploymentExist(eq("123456"))).thenReturn(false);
		worker.run(mockJob, callback);

		// Test inner exceptions during deployment
		accessJob.deploymentType = "Mock";
		worker.run(mockJob, callback);
	}
}
