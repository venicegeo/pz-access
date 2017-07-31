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
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.geotools.data.DataUtilities;
import org.geotools.data.memory.MemoryDataStore;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import com.mongodb.MongoException;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;

import access.controller.AccessController;
import access.database.DatabaseAccessor;
import access.deploy.Deployer;
import access.deploy.GroupDeployer;
import access.deploy.Leaser;
import access.messaging.AccessThreadManager;
import access.util.AccessUtilities;
import exception.GeoServerException;
import exception.InvalidInputException;
import model.data.DataResource;
import model.data.deployment.Deployment;
import model.data.deployment.DeploymentGroup;
import model.data.location.FolderShare;
import model.data.type.GeoJsonDataType;
import model.data.type.PostGISDataType;
import model.data.type.RasterDataType;
import model.data.type.TextDataType;
import model.response.DataResourceListResponse;
import model.response.DataResourceResponse;
import model.response.DeploymentGroupResponse;
import model.response.DeploymentListResponse;
import model.response.DeploymentResponse;
import model.response.ErrorResponse;
import model.response.PiazzaResponse;
import model.response.SuccessResponse;
import util.PiazzaLogger;

/**
 * Tests various parts of the Access component.
 * 
 * @author Patrick.Doody
 * 
 */
public class ControllerTests {
	@Mock
	private AccessThreadManager threadManager;
	@Mock
	private PiazzaLogger logger;
	@Mock
	private DatabaseAccessor accessor;
	@Mock
	private Deployer deployer;
	@Mock
	private GroupDeployer groupDeployer;
	@Mock
	private Leaser leaser;
	@Mock
	private ThreadPoolTaskExecutor threadPoolTaskExecutor;
	@Mock
	private AccessUtilities accessUtilities;
	@InjectMocks
	private AccessController accessController;

	private MemoryDataStore mockDataStore;

	private static final Logger LOGGER = LoggerFactory.getLogger(ControllerTests.class);

	/**
	 * Initialize Mock objects.
	 */
	@Before
	public void setup() throws SchemaException {
		MockitoAnnotations.initMocks(this);

		// Creating a Mock in-memory Data Store
		mockDataStore = new MemoryDataStore();
		SimpleFeatureType featureType = null;
		try {
			featureType = DataUtilities.createType("Test", "the_geom:Point");
		} catch (Exception exception) {
			LOGGER.error("Error inflating test in-memory Data Store. The tests cannot proceed.", exception);
			throw exception;
		}
		SimpleFeatureBuilder featureBuilder = new SimpleFeatureBuilder(featureType);
		GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();
		// Create some sample Test Points
		List<SimpleFeature> features = new ArrayList<SimpleFeature>();
		Point point = geometryFactory.createPoint(new Coordinate(5, 5));
		featureBuilder.add(point);
		SimpleFeature feature = featureBuilder.buildFeature(null);
		features.add(feature);
		Point otherPoint = geometryFactory.createPoint(new Coordinate(0, 0));
		featureBuilder.add(otherPoint);
		SimpleFeature otherFeature = featureBuilder.buildFeature(null);
		features.add(otherFeature);
		mockDataStore.addFeatures(features);
	}

	/**
	 * Tests the health check
	 */
	@Test
	public void testHealthCheck() {
		String response = accessController.getHealthCheck();
		assertTrue(response.contains("Health Check"));
	}

	/**
	 * Test an exception
	 */
	@Test
	public void testDownloadError() {
		// Mock no data being found
		when(accessor.getData(eq("123456"))).thenReturn(null);

		// Test
		accessController.accessFile("123456", "file.file");
	}

	/**
	 * Tests downloading a file
	 */
	@Test
	public void testDownloadFile() throws Exception {
		// Mock Text
		DataResource mockData = new DataResource();
		mockData.setDataId("123456");
		mockData.dataType = new TextDataType();
		((TextDataType) mockData.dataType).content = "This is a test";
		when(accessor.getData(eq("123456"))).thenReturn(mockData);
		ResponseEntity<?> response = accessController.accessFile("123456", "file.txt");

		// Verify
		assertTrue(response.getStatusCode().equals(HttpStatus.OK));
		assertTrue(new String((byte[]) response.getBody()).equals("This is a test"));

		// Mock Vector (Database)
		mockData.dataType = new PostGISDataType();
		((PostGISDataType) mockData.dataType).database = "localhost";
		((PostGISDataType) mockData.dataType).table = "Test";
		when(accessor.getData(eq("123456"))).thenReturn(mockData);
		when(accessor.getPostGisDataStore(anyString(), anyString(), anyString(), anyString(), anyString(), anyString()))
				.thenReturn(mockDataStore);
		response = accessController.accessFile("123456", "file.geojson");

		// Verify
		assertTrue(response.getStatusCode().equals(HttpStatus.OK));
		assertTrue(((byte[]) (response.getBody())).length > 0);
		// Check that the points exist in the response.
		String geoJson = new String((byte[]) response.getBody());
		assertTrue(geoJson.contains("[5,5]"));
		assertTrue(geoJson.contains("[0.0,0.0]"));

		// Mock File
		mockData.dataType = new RasterDataType();
		FolderShare location = new FolderShare();
		location.filePath = "src" + File.separator + "test" + File.separator + "resources" + File.separator + "elevation.tif";
		((RasterDataType) mockData.dataType).location = location;
		when(accessor.getData(eq("123456"))).thenReturn(mockData);
		Mockito.doReturn("Test".getBytes()).when(accessUtilities).getBytesForDataResource(Mockito.any(DataResource.class));
		response = accessController.accessFile("123456", "file.tif");

		// Verify
		assertTrue(response.getStatusCode().equals(HttpStatus.OK));
	}

	/**
	 * Tests GET /data/{dataId}
	 */
	@Test
	public void testGetData() {
		// Mock no data Id
		PiazzaResponse response = accessController.getData("").getBody();
		assertTrue(response instanceof ErrorResponse);

		// Mock no data
		when(accessor.getData(eq("123456"))).thenReturn(null);
		response = accessController.getData("123456").getBody();
		assertTrue(response instanceof ErrorResponse);

		// Proper mock
		DataResource mockData = new DataResource();
		mockData.setDataId("123456");
		mockData.dataType = new GeoJsonDataType();
		when(accessor.getData(eq("123456"))).thenReturn(mockData);

		// Test
		response = accessController.getData("123456").getBody();

		// Verify
		assertTrue(response instanceof DataResourceResponse);
		assertTrue(((DataResourceResponse) response).data.getDataId().equals("123456"));
	}

	/**
	 * Tests GET /deployment/{deploymentId}
	 */
	@Test
	public void testGetDeployment() {
		// Mock no deployment Id
		PiazzaResponse response = accessController.getDeployment("").getBody();
		assertTrue(response instanceof ErrorResponse);

		// Mock no deployment
		when(accessor.getDeployment(eq("123456"))).thenReturn(null);
		response = accessController.getDeployment("123456").getBody();
		assertTrue(response instanceof ErrorResponse);

		// Proper mock
		Deployment deployment = new Deployment();
		deployment.setDeploymentId("123456");
		when(accessor.getDeployment(eq("123456"))).thenReturn(deployment);

		// Test
		response = accessController.getDeployment("123456").getBody();

		// Verify
		assertTrue(response instanceof DeploymentResponse);
		assertTrue(((DeploymentResponse) response).data.getDeployment().getDeploymentId().equals("123456"));
	}

	/**
	 * Tests GET /data
	 */
	@Test
	public void testGetDataList() throws Exception {
		// Mock
		DataResourceListResponse mockResponse = new DataResourceListResponse();
		mockResponse.data = new ArrayList<DataResource>();
		mockResponse.data.add(new DataResource());
		when(accessor.getDataList(eq(0), eq(10), eq("dataId"), eq("asc"), eq("Raster"), eq("Test User"), eq("123")))
				.thenReturn(mockResponse);

		// Test
		PiazzaResponse response = accessController.getAllData("123", 0, 10, "dataId", "asc", "Raster", "Test User").getBody();

		// Verify
		assertTrue(response instanceof DataResourceListResponse);
		assertTrue(((DataResourceListResponse) response).data.size() == 1);

		// Test Exception
		Mockito.doThrow(new MongoException("MongoDB instance not available.")).when(accessor).getDataList(eq(0), eq(10), eq("dataId"),
				eq("asc"), eq("Raster"), eq("Test User"), eq("123"));
		response = accessController.getAllData("123", 0, 10, "dataId", "asc", "Raster", "Test User").getBody();
		assertTrue(response instanceof ErrorResponse);
	}

	/**
	 * Tests GET /deployment
	 */
	@Test
	public void testDeploymentList() throws Exception {
		// Mock
		DeploymentListResponse mockResponse = new DeploymentListResponse();
		mockResponse.data = new ArrayList<Deployment>();
		mockResponse.data.add(new Deployment());
		when(accessor.getDeploymentList(eq(0), eq(10), eq("dataId"), eq("asc"), eq("WFS"))).thenReturn(mockResponse);

		// Test
		PiazzaResponse response = accessController.getAllDeployments(0, 10, "dataId", "asc", "WFS").getBody();

		// Verify
		assertTrue(response instanceof DeploymentListResponse);
		assertTrue(((DeploymentListResponse) response).data.size() == 1);

		// Test Exception
		Mockito.doThrow(new MongoException("MongoDB instance not available.")).when(accessor).getDeploymentList(eq(0), eq(10), eq("dataId"),
				eq("asc"), eq("WFS"));
		response = accessController.getAllDeployments(0, 10, "dataId", "asc", "WFS").getBody();
		assertTrue(response instanceof ErrorResponse);
	}

	/**
	 * Tests GET /data
	 */
	@Test
	public void testDataCount() {
		// Mock
		when(accessor.getDataCount()).thenReturn((long) 5000);
		// Test
		long count = accessController.getDataCount();
		// Verify
		assertTrue(count == 5000);
	}

	/**
	 * Test DELETE /deployment/{deploymentId} and /reap
	 */
	@Test
	public void testUndeploy() throws Exception {
		// Test undeploying
		Deployment deployment = new Deployment();
		deployment.setDeploymentId("123456");
		when(accessor.getDeployment(eq("123456"))).thenReturn(deployment);
		ResponseEntity<PiazzaResponse> response = accessController.deleteDeployment("123456");
		assertTrue(response.getStatusCode().compareTo(HttpStatus.OK) == 0);

		// Test Exception
		Mockito.doThrow(new GeoServerException("Test")).when(deployer).undeploy(eq("123456"));
		response = accessController.deleteDeployment("123456");
		assertTrue(response.getBody() instanceof ErrorResponse);

		// Test reaping
		accessController.forceReap();
	}

	/**
	 * Test GET /admin/stats
	 */
	@Test
	public void testAdminStats() {
		// Test
		ResponseEntity<Map<String, Object>> response = accessController.getAdminStats();
		Map<String, Object> stats = response.getBody();

		// Verify
		assertTrue(stats != null);
		assertTrue(stats.keySet().contains("jobs"));
	}

	/**
	 * Tests a Deployment Group
	 */
	@Test
	public void testCreateGroup() {
		// Correct output
		Mockito.doReturn(new DeploymentGroup("123456", "Tester")).when(groupDeployer).createDeploymentGroup("Tester");
		ResponseEntity<PiazzaResponse> response = accessController.createDeploymentGroup("Tester");
		assertTrue(response.getBody() instanceof DeploymentGroupResponse);
	}

	/**
	 * Test deleting a deployment group
	 */
	@Test
	public void testDeleteGroup() throws Exception {
		// Test a null input
		ResponseEntity<PiazzaResponse> response = accessController.deleteDeploymentGroup(null);
		assertTrue(response.getBody() instanceof ErrorResponse);

		// Test a null Deployment Group
		response = accessController.deleteDeploymentGroup("123456");
		assertTrue(response.getBody() instanceof ErrorResponse);

		DeploymentGroup mockGroup = new DeploymentGroup("123456", "Tester");
		Mockito.doReturn(mockGroup).when(accessor).getDeploymentGroupById("123456");

		// Test correct response
		response = accessController.deleteDeploymentGroup("123456");
		assertTrue(response.getBody() instanceof SuccessResponse);

		// Test an Exception
		Mockito.doThrow(new GeoServerException("Error")).when(groupDeployer).deleteDeploymentGroup(Mockito.any());
		response = accessController.deleteDeploymentGroup("123456");
		assertTrue(response.getBody() instanceof ErrorResponse);
	}
}
