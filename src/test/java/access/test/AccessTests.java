package access.test;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

import java.util.UUID;

import model.data.DataResource;
import model.data.type.TextDataType;
import model.job.metadata.ResourceMetadata;
import model.job.metadata.SpatialMetadata;
import model.response.DataResourceResponse;
import model.response.PiazzaResponse;
import model.security.SecurityClassification;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import util.PiazzaLogger;
import access.controller.AccessController;
import access.database.MongoAccessor;

/**
 * Tests various parts of the Access component.
 * 
 * @author Patrick.Doody
 * 
 */
public class AccessTests {
	@Mock
	private PiazzaLogger logger;
	@Mock
	private MongoAccessor accessor;
	@InjectMocks
	private AccessController accessController;

	/**
	 * Initialize Mock objects.
	 */
	@Before
	public void setup() {
		MockitoAnnotations.initMocks(this);
	}

	@Test
	public void getDataTest() {
		// Mock a DataResource object to request for. Populate with test
		// metadata.
		TextDataType textData = new TextDataType();
		textData.content = "This is a test data.";
		textData.mimeType = "application/text";

		DataResource mockData = new DataResource();
		mockData.dataId = UUID.randomUUID().toString();
		mockData.dataType = textData;

		SpatialMetadata spatialMetadata = new SpatialMetadata();
		spatialMetadata.setEpsgCode(4326);
		mockData.spatialMetadata = spatialMetadata;

		ResourceMetadata metadata = new ResourceMetadata();
		metadata.name = "Test data";
		metadata.description = "Mockito Test Data";
		metadata.classType = new SecurityClassification("unclassified");
		mockData.metadata = metadata;

		// Mock the query to respond with the Mock DataResource item
		when(accessor.getData(anyString())).thenReturn(mockData);

		// Fetch the metadata
		PiazzaResponse response = accessController.getData(mockData.dataId);
		assertTrue(response instanceof DataResourceResponse);
		DataResourceResponse dataResponse = (DataResourceResponse) response;
		// Verify the contents of the response match the metadata inputs
		assertTrue(dataResponse.data.dataId.equals(mockData.dataId));
		assertTrue(dataResponse.data.metadata.classType.getClassification().equals(
				metadata.classType.getClassification()));
		assertTrue(dataResponse.data.metadata.name.equals(metadata.name));
		assertTrue(dataResponse.data.metadata.description.equals(metadata.description));
		assertTrue(dataResponse.data.spatialMetadata.getEpsgCode().equals(spatialMetadata.getEpsgCode()));
		assertTrue(dataResponse.data.dataType.getType().equals("text"));
		TextDataType responseTextData = (TextDataType) dataResponse.data.dataType;
		assertTrue(responseTextData.content.equals(textData.content));
		assertTrue(responseTextData.mimeType.equals(textData.mimeType));
	}
}
