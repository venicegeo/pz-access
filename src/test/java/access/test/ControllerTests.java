package access.test;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

import java.io.File;

import model.data.DataResource;
import model.data.location.FolderShare;
import model.data.type.PostGISDataType;
import model.data.type.RasterDataType;
import model.data.type.TextDataType;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import util.GeoToolsUtil;
import util.PiazzaLogger;
import access.controller.AccessController;
import access.database.MongoAccessor;
import access.deploy.Deployer;
import access.deploy.Leaser;
import access.messaging.AccessThreadManager;

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
	private MongoAccessor accessor;
	@Mock
	private Deployer deployer;
	@Mock
	private Leaser leaser;
	@Mock
	private GeoToolsUtil geoToolsUtil;
	@InjectMocks
	private AccessController accessController;

	/**
	 * Initialize Mock objects.
	 */
	@Before
	public void setup() {
		MockitoAnnotations.initMocks(this);
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
		((PostGISDataType) mockData.dataType).table = "test";

		// Mock File
		mockData.dataType = new RasterDataType();
		FolderShare location = new FolderShare();
		location.filePath = "src" + File.separator + "test" + File.separator + "resources" + File.separator
				+ "elevation.tif";
		((RasterDataType) mockData.dataType).location = location;
	}
}
