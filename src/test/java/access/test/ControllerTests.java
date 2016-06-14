package access.test;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import model.data.DataResource;
import model.data.location.FolderShare;
import model.data.type.PostGISDataType;
import model.data.type.RasterDataType;
import model.data.type.TextDataType;

import org.geotools.data.DataUtilities;
import org.geotools.data.memory.MemoryDataStore;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import util.GeoToolsUtil;
import util.PiazzaLogger;
import access.controller.AccessController;
import access.database.Accessor;
import access.deploy.Deployer;
import access.deploy.Leaser;
import access.messaging.AccessThreadManager;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;

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
	private Accessor accessor;
	@Mock
	private Deployer deployer;
	@Mock
	private Leaser leaser;
	@InjectMocks
	private AccessController accessController;

	private MemoryDataStore mockDataStore;

	/**
	 * Initialize Mock objects.
	 */
	@Before
	public void setup() throws Exception {
		MockitoAnnotations.initMocks(this);

		// Creating a Mock in-memory Data Store
		mockDataStore = new MemoryDataStore();
		SimpleFeatureType featureType = DataUtilities.createType("Test", "the_geom:Point:srid=4326");
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
	@Test(expected = Exception.class)
	public void testDownloadError() throws Exception {
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
		ResponseEntity<byte[]> response = accessController.accessFile("123456", "file.txt");

		// Verify
		assertTrue(response.getStatusCode().equals(HttpStatus.OK));
		assertTrue(new String(response.getBody()).equals("This is a test"));

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
		assertTrue(response.getBody().length > 0);
		// Check that the points exist in the response.
		String geoJson = new String(response.getBody());
		assertTrue(geoJson.contains("[5,5]"));
		assertTrue(geoJson.contains("[0.0,0.0]"));

		// Mock File
		mockData.dataType = new RasterDataType();
		FolderShare location = new FolderShare();
		location.filePath = "src" + File.separator + "test" + File.separator + "resources" + File.separator
				+ "elevation.tif";
		((RasterDataType) mockData.dataType).location = location;
		when(accessor.getData(eq("123456"))).thenReturn(mockData);
		response = accessController.accessFile("123456", "file.tif");

		// Verify
		assertTrue(response.getStatusCode().equals(HttpStatus.OK));
		assertTrue(response.getBody().length == 90074);

	}
}
