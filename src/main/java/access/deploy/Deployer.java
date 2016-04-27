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
package access.deploy;

import java.io.IOException;

import model.data.DataResource;
import model.data.deployment.Deployment;
import model.data.location.FileLocation;
import model.data.location.S3FileStore;
import model.data.type.PostGISDataType;
import model.data.type.RasterDataType;
import model.data.type.ShapefileDataType;
import model.data.type.WfsDataType;

import org.apache.commons.io.IOUtils;
import org.apache.tomcat.util.codec.binary.Base64;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import util.PiazzaLogger;
import util.UUIDFactory;
import access.database.MongoAccessor;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;

/**
 * Class that manages the GeoServer Deployments held by this component. This is
 * done by managing the Deployments via a MongoDB collection.
 * 
 * A deployment is, in this current context, a GeoServer layer being stood up.
 * In the future, this may be expanded to other deployment solutions, as
 * requested by users in the Access Job.
 * 
 * @author Patrick.Doody
 * 
 */
@Component
public class Deployer {
	@Autowired
	private PiazzaLogger logger;
	@Autowired
	private UUIDFactory uuidFactory;
	@Autowired
	private MongoAccessor accessor;
	@Value("${vcap.services.pz-geoserver.credentials.geoserver.hostname}")
	private String GEOSERVER_HOST;
	@Value("${vcap.services.pz-geoserver.credentials.geoserver.port}")
	private String GEOSERVER_PORT;
	@Value("${vcap.services.pz-geoserver.credentials.geoserver.username}")
	private String GEOSERVER_USERNAME;
	@Value("${vcap.services.pz-geoserver.credentials.geoserver.password}")
	private String GEOSERVER_PASSWORD;
	@Value("${vcap.services.pz-geoserver.credentials.s3.bucket}")
	private String GEOSERVER_DATA_DIRECTORY;
	@Value("${vcap.services.pz-geoserver.credentials.s3.access_key_id}")
	private String AMAZONS3_ACCESS_KEY;
	@Value("${vcap.services.pz-geoserver.credentials.s3.secret_access_key}")
	private String AMAZONS3_PRIVATE_KEY;

	private static final String HOST_ADDRESS = "http://%s:%s%s";
	private static final String GEOSERVER_DEFAULT_WORKSPACE = "piazza";

	private static final String ADD_LAYER_ENDPOINT = "/geoserver/rest/workspaces/piazza/datastores/piazza/featuretypes/";
	private static final String CAPABILITIES_URL = "/geoserver/piazza/wfs?service=wfs&version=2.0.0&request=GetCapabilities";

	private static final String DATA_STORE_ENDPOINT = "/geoserver/rest/workspaces/piazza/coveragestores";
	private static final String LAYER_REST_ENDPOINT = "/geoserver/rest/workspaces/piazza/coveragestores/%s/coverages";

	/**
	 * Creates a new deployment from the dataResource object.
	 * 
	 * @param dataResource
	 *            The resource metadata, describing the object to be deployed.
	 * @return A deployment for the object.
	 */
	public Deployment createDeployment(DataResource dataResource) throws Exception {
		// Create the GeoServer Deployment based on the Data Type
		Deployment deployment;
		try {
			if ((dataResource.getDataType() instanceof ShapefileDataType)
					|| (dataResource.getDataType() instanceof PostGISDataType)) {
				// Deploy from an existing PostGIS Table
				deployment = deployPostGisTable(dataResource);
			} else if (dataResource.getDataType() instanceof WfsDataType) {
				// User has requested to deploy a WFS type resource. In this
				// case, there's nothing to deploy since the WFS is already
				// accessible by design? Just return the WFS information back to
				// them? Or return an error?
				deployment = null;
			} else if (dataResource.getDataType() instanceof RasterDataType) {
				// Deploy a GeoTIFF to GeoServer
				deployment = deployGeoTiff(dataResource);
			} else {
				// Unsupported Data type has been specified.
				throw new UnsupportedOperationException("Cannot the following Data Type to GeoServer: "
						+ dataResource.getDataType().getType());
			}
		} catch (Exception exception) {
			exception.printStackTrace();
			throw new Exception("There was an error deploying the to GeoServer instance: " + exception.getMessage());
		}

		// Insert the Deployment into the Database
		accessor.insertDeployment(deployment);

		// Log information
		logger.log(
				String.format("Created Deployment %s for Data %s on host %s", deployment.getId(),
						deployment.getDataId(), deployment.getHost()), PiazzaLogger.INFO);

		// Return Deployment reference
		return deployment;
	}

	/**
	 * Deploys a PostGIS Table resource to GeoServer. This will create a new
	 * GeoServer layer that will reference the PostGIS table.
	 * 
	 * PostGIS tables can be created via the Ingest process by, for instance,
	 * ingesting a Shapefile or a WFS into the Database.
	 * 
	 * @param dataResource
	 *            The DataResource to deploy.
	 * @return The Deployment
	 */
	private Deployment deployPostGisTable(DataResource dataResource) throws Exception {
		// Create the JSON Payload for the Layer request to GeoServer
		ClassLoader classLoader = getClass().getClassLoader();
		String featureTypeRequestBody = IOUtils.toString(classLoader
				.getResourceAsStream("templates/featureTypeRequest.xml"));

		// Get the appropriate Table Name from the DataResource
		String tableName = null;
		if (dataResource.getDataType() instanceof ShapefileDataType) {
			tableName = ((ShapefileDataType) dataResource.getDataType()).getDatabaseTableName();
		} else if (dataResource.getDataType() instanceof PostGISDataType) {
			tableName = ((PostGISDataType) dataResource.getDataType()).getTable();
		}

		// Inject the Metadata from the Data Resource into the Payload
		String requestBody = String.format(featureTypeRequestBody, tableName, tableName, tableName, dataResource
				.getSpatialMetadata().getEpsgString(), "EPSG:4326");

		// Execute the POST to GeoServer to add the FeatureType
		HttpStatus statusCode = postGeoServerFeatureType(ADD_LAYER_ENDPOINT, requestBody);

		// Ensure the Status Code is OK
		if (statusCode != HttpStatus.CREATED) {
			logger.log(String.format(
					"Failed to Deploy PostGIS Table name %s for Resource %s to GeoServer. HTTP Code: ", tableName,
					dataResource.getDataId(), statusCode), PiazzaLogger.ERROR);
			throw new Exception("Failed to Deploy to GeoServer; the Status returned a non-OK response code: "
					+ statusCode);
		}

		// Create a new Deployment for this Resource
		String deploymentId = uuidFactory.getUUID();
		String capabilitiesUrl = String.format(HOST_ADDRESS, GEOSERVER_HOST, GEOSERVER_PORT, CAPABILITIES_URL);

		Deployment deployment = new Deployment(deploymentId, dataResource.getDataId(), GEOSERVER_HOST, GEOSERVER_PORT,
				tableName, capabilitiesUrl);

		// Return the newly created Deployment
		return deployment;
	}

	/**
	 * Will copy file to geoserver data directory, and return the file name.
	 * 
	 * @param fileLocation
	 *            Interface to get file info from.
	 * @return String Path to file
	 * @throws Exception
	 * @throws IOException
	 */
	private String copyFileToGeoServerData(FileLocation fileLocation) throws IOException, Exception {
		// Ensure the FileLocation is an S3 Bucket. Otherwise, we're not able to
		// deploy.
		if (fileLocation instanceof S3FileStore == false) {
			throw new Exception("Cannot deploy this location because it does not reside in S3.");
		}
		S3FileStore fileStore = (S3FileStore) fileLocation;
		// Get AWS Client
		AmazonS3 s3client = new AmazonS3Client(new BasicAWSCredentials(AMAZONS3_ACCESS_KEY, AMAZONS3_PRIVATE_KEY));
		// Copy the file to the GeoServer S3 Bucket
		logger.log(
				String.format(
						"Preparing to deploy Raster service. Moving file %s:%s from Piazza bucket into GeoServer bucket at %s:%s",
						fileStore.getBucketName(), fileStore.getFileName(), GEOSERVER_DATA_DIRECTORY,
						fileStore.getFileName()), PiazzaLogger.INFO);
		s3client.copyObject(fileStore.getBucketName(), fileStore.getFileName(), GEOSERVER_DATA_DIRECTORY,
				fileStore.getFileName());
		// File name
		return fileStore.getFileName();
	}

	/**
	 * Deploys a GeoTIFF resource to GeoServer. This will create a new GeoServer
	 * data store and layer. GeoTIFF file assumed to reside under data directory
	 * of GeoServer at this point.
	 * 
	 * @param dataResource
	 *            The DataResource to deploy.
	 * @return The Deployment
	 */
	private Deployment deployGeoTiff(DataResource dataResource) throws Exception {
		// Copy GeoTIFF from AWS S3 to Data Directory of GeoServer
		FileLocation fileLocation = ((RasterDataType) dataResource.getDataType()).getLocation();
		String dataStoreFileName = copyFileToGeoServerData(fileLocation);

		// Create Data Store in GeoServer for a given resource
		createGeoTiffDataStore(dataResource, GEOSERVER_DEFAULT_WORKSPACE, dataStoreFileName);

		// Create Layer in GeoServer for a given resource
		createCoverageLayer(dataResource, GEOSERVER_DEFAULT_WORKSPACE);

		// Create a new Deployment for this Resource
		String deploymentId = uuidFactory.getUUID();
		String capabilitiesUrl = String.format(HOST_ADDRESS, GEOSERVER_HOST, GEOSERVER_PORT, CAPABILITIES_URL);
		String deploymentLayerName = fileLocation.getFileName() + "-" + dataResource.getDataId();
		Deployment deployment = new Deployment(deploymentId, dataResource.getDataId(), GEOSERVER_HOST, GEOSERVER_PORT,
				deploymentLayerName, capabilitiesUrl);

		// Return the newly created Deployment
		return deployment;
	}

	/**
	 * Executes the POST request to GeoServer to create the new layer
	 * 
	 * @param dataStoreName
	 *            The name of the new data store
	 * @param dataStoreDescription
	 *            The description of the new data store
	 * @param workspace
	 *            The workspace to use to place data store under
	 * @param dataStoreFileName
	 *            The file name of the raster file to use for data store
	 * 
	 */
	private void createGeoTiffDataStore(DataResource dataResource, String workspaceName, String dataStoreFileName)
			throws Exception {
		// Load template
		ClassLoader classLoader = getClass().getClassLoader();
		String dataStoreTemplate = IOUtils.toString(classLoader
				.getResourceAsStream("templates/coverageStoreTypeRequest.xml"));

		// Inject Metadata from the Data Resource into the Data Store Payload.
		// Log this to console for traceability.
		String dataStoreRequestBody = String.format(dataStoreTemplate, dataResource.getDataId(),
				"piazza generated data store", workspaceName, dataStoreFileName);
		System.out.println(dataStoreRequestBody);

		// Execute the POST to GeoServer to add the data store
		HttpStatus statusCode = postGeoServerFeatureType(DATA_STORE_ENDPOINT, dataStoreRequestBody);

		// Ensure the Status Code is OK
		if (statusCode != HttpStatus.CREATED) {
			logger.log(String.format("Failed to create Data Source on for Resource %s to GeoServer. HTTP Code: %s",
					dataResource.getDataId(), statusCode), PiazzaLogger.ERROR);
			throw new Exception(
					"Failed to Deploy GeoTIFF data store to GeoServer; the Status returned a non-OK response code: "
							+ statusCode);
		}
	}

	/**
	 * Executes the POST request to GeoServer to create the new layer
	 * 
	 * @param dataStoreName
	 *            The name of the new data store
	 * @param workspaceName
	 *            The name of the workspace where data store will be placed
	 * @param coverage
	 *            The GridCoverage2D to grab data from
	 * @param layerName
	 *            The name of the new layer
	 * @param layerTitle
	 *            The title of the new layer
	 * @param layerDescription
	 *            The description of the new layer
	 * 
	 */
	private void createCoverageLayer(DataResource dataResource, String workspaceName) throws Exception {
		// Load template
		ClassLoader classLoader = getClass().getClassLoader();
		String layerTemplate = IOUtils.toString(classLoader.getResourceAsStream("templates/coverageTypeRequest.xml"));

		// Obtain NativeBoundingBox Data
		double minX = dataResource.getSpatialMetadata().getMinX();
		double maxX = dataResource.getSpatialMetadata().getMaxX();
		double minY = dataResource.getSpatialMetadata().getMinY();
		double maxY = dataResource.getSpatialMetadata().getMaxY();

		// Obtain Coordinate Reference System Data
		String coordinateReferenceSystemData = dataResource.getSpatialMetadata().getCoordinateReferenceSystem();
		Integer epsgCode = dataResource.getSpatialMetadata().getEpsgCode();

		// Inject the Metadata from the Data Resource into the Payload
		String layerRequestBody = String
				.format(layerTemplate, dataResource.getDataId(), dataResource.getDataId(), workspaceName,
						dataResource.getDataId(), "piazza generated layer", coordinateReferenceSystemData, epsgCode,
						minX, maxX, minY, maxY, epsgCode, workspaceName, dataResource.getDataId(), epsgCode, epsgCode);

		// Execute the POST to GeoServer to create the layer
		String layerRestEndpoint = String.format(LAYER_REST_ENDPOINT, dataResource.getDataId());
		HttpStatus statusCode = postGeoServerFeatureType(layerRestEndpoint, layerRequestBody);

		// Ensure the Status Code is OK
		if (statusCode != HttpStatus.CREATED) {
			logger.log(
					String.format("Failed to create layer for Resource %s on GeoServer. HTTP Code: %s",
							dataResource.getDataId(), statusCode), PiazzaLogger.ERROR);
			throw new Exception(
					"Failed to Deploy GeoTIFF layer to GeoServer; the Status returned a non-OK response code: "
							+ statusCode);
		}
	}

	/**
	 * Executes the POST request to GeoServer to create the FeatureType as a
	 * Layer.
	 * 
	 * @param featureType
	 *            The JSON Payload of the POST request
	 * @return The HTTP Status code of the request to GeoServer for adding the
	 *         layer. GeoServer will typically not return any payload in the
	 *         response, so the HTTP Status is the best we can do in order to
	 *         check for success.
	 */
	private HttpStatus postGeoServerFeatureType(String restURL, String featureType) {
		// Get the Basic authentication Headers for GeoServer
		String plainCredentials = String.format("%s:%s", GEOSERVER_USERNAME, GEOSERVER_PASSWORD);
		byte[] credentialBytes = plainCredentials.getBytes();
		byte[] encodedCredentials = Base64.encodeBase64(credentialBytes);
		String credentials = new String(encodedCredentials);
		HttpHeaders headers = new HttpHeaders();
		headers.add("Authorization", "Basic " + credentials);
		headers.setContentType(MediaType.APPLICATION_XML);

		// Construct the URL for the Service
		String url = String.format(HOST_ADDRESS, GEOSERVER_HOST, GEOSERVER_PORT, restURL);

		// Create the Request template and execute
		HttpEntity<String> request = new HttpEntity<String>(featureType, headers);

		RestTemplate restTemplate = new RestTemplate();
		ResponseEntity<String> response = restTemplate.exchange(url, HttpMethod.POST, request, String.class);

		// Return the HTTP Status
		return response.getStatusCode();
	}

	/**
	 * Checks to see if the DataResource currently has a deployment in the
	 * system or not.
	 * 
	 * @param dataId
	 *            The Data ID to check for Deployment.
	 * @return True if a deployment exists for the Data ID, false if not.
	 */
	public boolean doesDeploymentExist(String dataId) {
		Deployment deployment = accessor.getDeploymentByDataId(dataId);
		if (deployment != null) {
			return true;
		} else {
			return false;
		}
	}
}
