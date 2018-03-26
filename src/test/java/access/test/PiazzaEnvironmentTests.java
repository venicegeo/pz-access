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
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import java.util.ArrayList;

import org.geotools.data.DataStore;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.client.RestTemplate;
import org.venice.piazza.common.hibernate.dao.DeploymentGroupDao;
import org.venice.piazza.common.hibernate.dao.LeaseDao;
import org.venice.piazza.common.hibernate.dao.dataresource.DataResourceDao;
import org.venice.piazza.common.hibernate.dao.deployment.DeploymentDao;
import org.venice.piazza.common.hibernate.entity.DataResourceEntity;
import org.venice.piazza.common.hibernate.entity.DeploymentEntity;

import access.database.DatabaseAccessor;
import access.deploy.geoserver.AuthHeaders;
import access.deploy.geoserver.PiazzaEnvironment;
import model.data.DataResource;
import model.data.deployment.Deployment;
import util.GeoToolsUtil;
import util.PiazzaLogger;

/**
 * Tests the Database Accessor CRUD Utility
 * 
 * @author Sonny.Saniev
 *
 */
public class PiazzaEnvironmentTests {

	@Mock
	private RestTemplate restTemplate;
	@Mock
	private AuthHeaders authHeaders;
	@Mock
	private PiazzaLogger pzLogger;
	@InjectMocks
	private PiazzaEnvironment piazzaEnvironment;
	
	/**
	 * Test initialization
	 */
	@Before
	public void setup() {
		MockitoAnnotations.initMocks(this);
	}

	@Test
	public void testGetPostGisDataStore() throws Exception {
		piazzaEnvironment.initializeEnvironment();
		assertTrue(true); // no error occurred
	}
}
