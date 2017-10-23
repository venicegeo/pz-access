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

import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import access.database.DatabaseAccessor;
import access.deploy.Deployer;
import access.deploy.GroupDeployer;
import access.deploy.geoserver.AuthHeaders;
import model.data.deployment.Deployment;
import model.data.deployment.DeploymentGroup;
import util.PiazzaLogger;
import util.UUIDFactory;

/**
 * Tests the Group Deployer component.
 * 
 * @author Patrick.Doody
 *
 */
public class GroupDeployerTests {
	@Mock
	private PiazzaLogger logger;
	@Mock
	private UUIDFactory uuidFactory;
	@Mock
	private DatabaseAccessor accessor;
	@Mock
	private Deployer deployer;
	@Mock
	private RestTemplate restTemplate;
	@Mock
	private AuthHeaders geoServerHeaders;
	@InjectMocks
	private GroupDeployer groupDeployer;

	private Deployment mockDeployment;
	private DeploymentGroup mockGroup;

	/**
	 * Test initialization
	 */
	@Before
	public void setup() {
		MockitoAnnotations.initMocks(this);

		// UUID Factory always generates a GUID
		when(uuidFactory.getUUID()).thenReturn("123456");

		mockDeployment = new Deployment("123456", "123456", "localhost", "8080", "layer", "getCapabilities");
		mockGroup = new DeploymentGroup("123456", "Tester");
	}

	/**
	 * Test group creation
	 */
	@Test
	public void testCreateGroup() {
		DeploymentGroup group = groupDeployer.createDeploymentGroup("Tester");
		Assert.assertTrue(group.getHasGisServerLayer().equals(false));
		Assert.assertTrue(group.createdBy.equals("Tester"));
		Assert.assertTrue(group.deploymentGroupId.equals("123456"));
	}

	/**
	 * Test creation of a group and insertion into GeoServer
	 */
	@Test
	public void testCreateGroupGeoServer() throws Exception {
		// Mock
		List<Deployment> deployments = new ArrayList<Deployment>();
		deployments.add(mockDeployment);
		Mockito.doReturn(new ResponseEntity<String>("OK", HttpStatus.CREATED)).when(restTemplate).exchange(Mockito.anyString(),
				Mockito.any(), Mockito.any(), Mockito.eq(String.class));

		// Test
		DeploymentGroup deploymentGroup = groupDeployer.createDeploymentGroup(deployments, "Tester");

		// Assert
		Assert.assertTrue(deploymentGroup instanceof DeploymentGroup);
		Assert.assertTrue(deploymentGroup.deploymentGroupId.equals("123456"));
		Assert.assertTrue(deploymentGroup.getHasGisServerLayer().equals(true));
	}

	/**
	 * Tests updating a Deployment Group
	 */
	@Test
	public void testUpdateLayerGroup() throws Exception {
		// Mock
		List<Deployment> deployments = new ArrayList<Deployment>();
		deployments.add(mockDeployment);
		mockGroup.setHasGisServerLayer(true);

		String mockXmlResponse = "<layerGroup><name>a451b8fa-17b3-44ef-8435-77c89eba70ad</name><mode>SINGLE</mode><publishables><published type=\"layer\"><name>3b49ec41-9ea7-412c-8406-781abe39a8f2</name></published></publishables><styles><style/></styles></layerGroup>";
		Mockito.doReturn(new ResponseEntity<String>(mockXmlResponse, HttpStatus.OK)).when(restTemplate).exchange(Mockito.anyString(),
				Mockito.any(), Mockito.any(), Mockito.eq(String.class));

		// Test
		groupDeployer.updateDeploymentGroup(mockGroup, deployments);
	}

	/**
	 * Deletes a Group
	 */
	@Test
	public void testDeleteGroup() throws Exception {
		// Mock
		Mockito.doReturn(new ResponseEntity<String>("OK", HttpStatus.OK)).when(restTemplate).exchange(Mockito.anyString(), Mockito.any(),
				Mockito.any(), Mockito.eq(String.class));

		// Test
		groupDeployer.deleteDeploymentGroup(mockGroup);
	}

	/**
	 * Deletes a group - with exception
	 */
	@Test(expected = Exception.class)
	public void testDeleteException() throws Exception {
		Mockito.doThrow(new HttpClientErrorException(HttpStatus.INTERNAL_SERVER_ERROR)).when(restTemplate).exchange(Mockito.anyString(),
				Mockito.any(), Mockito.any(), Mockito.eq(String.class));

		// Test
		groupDeployer.deleteDeploymentGroup(mockGroup);
	}
}
