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

import java.io.File;

import model.data.DataResource;
import model.data.deployment.Deployment;
import model.data.type.GeoJsonDataType;
import model.data.type.PostGISDataType;
import model.data.type.RasterDataType;
import model.data.type.ShapefileDataType;

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
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import util.PiazzaLogger;
import util.UUIDFactory;
import access.database.Accessor;
import access.util.AccessUtilities;

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
	private AccessUtilities accessUtilities;
	@Autowired
	private Accessor accessor;
	@Value("${vcap.services.pz-geoserver.credentials.geoserver.hostname}")
	private String GEOSERVER_HOST;
	@Value("${vcap.services.pz-geoserver.credentials.geoserver.port}")
	private String GEOSERVER_PORT;
	@Value("${vcap.services.pz-geoserver.credentials.geoserver.username}")
	private String GEOSERVER_USERNAME;
	@Value("${vcap.services.pz-geoserver.credentials.geoserver.password}")
	private String GEOSERVER_PASSWORD;

	private RestTemplate restTemplate = new RestTemplate();

	private static final String HOST_ADDRESS = "http://%s:%s%s";

	private static final String ADD_LAYER_ENDPOINT = "/geoserver/rest/workspaces/piazza/datastores/piazza/featuretypes/";
	private static final String CAPABILITIES_URL = "/geoserver/piazza/wfs?service=wfs&version=2.0.0&request=GetCapabilities";

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
					|| (dataResource.getDataType() instanceof PostGISDataType)
					|| (dataResource.getDataType() instanceof GeoJsonDataType)) {
				// Deploy from an existing PostGIS Table
				deployment = deployPostGisTable(dataResource);
			} else if (dataResource.getDataType() instanceof RasterDataType) {
				// Deploy a GeoTIFF to GeoServer
				deployment = deployRaster(dataResource);
			} else {
				// Unsupported Data type has been specified.
				throw new UnsupportedOperationException("Cannot deploy the following Data Type to GeoServer: "
						+ dataResource.getDataType().getClass().getSimpleName());
			}
		} catch (Exception exception) {
			exception.printStackTrace();
			throw new Exception("There was an error deploying the to GeoServer instance: " + exception.getMessage());
		}

		// Insert the Deployment into the Database
		accessor.insertDeployment(deployment);

		// Log information
		logger.log(
				String.format("Created Deployment %s for Data %s on host %s", deployment.getDeploymentId(),
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
		String featureTypeRequestBody = IOUtils.toString(classLoader.getResourceAsStream("templates" + File.separator
				+ "featureTypeRequest.xml"));

		// Get the appropriate Table Name from the DataResource
		String tableName = null;
		if (dataResource.getDataType() instanceof ShapefileDataType) {
			tableName = ((ShapefileDataType) dataResource.getDataType()).getDatabaseTableName();
		} else if (dataResource.getDataType() instanceof PostGISDataType) {
			tableName = ((PostGISDataType) dataResource.getDataType()).getTable();
		} else if (dataResource.getDataType() instanceof GeoJsonDataType) {
			tableName = ((GeoJsonDataType) dataResource.getDataType()).databaseTableName;
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
	 * Deploys a GeoTIFF resource to GeoServer. This will create a new GeoServer
	 * data store and layer. This will upload the file directly to GeoServer
	 * using the GeoServer REST API.
	 * 
	 * @param dataResource
	 *            The DataResource to deploy.
	 * @return The Deployment
	 */
	private Deployment deployRaster(DataResource dataResource) throws Exception {
		// Get the File Bytes of the Raster to be uploaded
		byte[] fileBytes = accessUtilities.getBytesForDataResource(dataResource);

		// Create the Request that will upload the File
		HttpHeaders headers = getGeoServerHeaders();
		headers.add("Content-type", "image/tiff");
		HttpEntity<byte[]> request = new HttpEntity<byte[]>(fileBytes, headers);

		// Send the Request
		String url = String.format("http://%s:%s/geoserver/rest/workspaces/piazza/coveragestores/%s/file.geotiff",
				"localhost", "8080", dataResource.getDataId());
		ResponseEntity<String> response = restTemplate.exchange(url, HttpMethod.PUT, request, String.class);
		if (response.getStatusCode().equals(HttpStatus.CREATED) == false) {
			throw new Exception(String.format("Creating Layer on GeoServer returned HTTP Status %s with Body: %s",
					response.getStatusCode().toString(), response.getBody()));
		}

		// Create a Deployment for this Resource
		String deploymentId = uuidFactory.getUUID();
		String capabilitiesUrl = String.format(HOST_ADDRESS, GEOSERVER_HOST, GEOSERVER_PORT, CAPABILITIES_URL);
		String deploymentLayerName = dataResource.getDataId();
		Deployment deployment = new Deployment(deploymentId, dataResource.getDataId(), GEOSERVER_HOST, GEOSERVER_PORT,
				deploymentLayerName, capabilitiesUrl);

		// Return the newly Created Deployment
		return deployment;
	}

	/**
	 * Deletes a deployment, as specified by its ID. This will remove the
	 * Deployment from GeoServer, delete the lease and the deployment from the
	 * Database.
	 * 
	 * @param deploymentId
	 *            The ID of the deployment.
	 */
	public void undeploy(String deploymentId) throws Exception {
		// Get the Deployment from the Database to delete. If the Deployment had
		// a lease, then the lease is automatically removed when the deployment
		// is deleted.
		Deployment deployment = accessor.getDeployment(deploymentId);
		if (deployment == null) {
			throw new Exception("Deployment does not exist matching ID " + deploymentId);
		}
		// Delete the Deployment from GeoServer
		HttpHeaders headers = getGeoServerHeaders();
		headers.setContentType(MediaType.APPLICATION_XML);
		HttpEntity<String> request = new HttpEntity<String>(headers);
		String url = String.format("http://%s:%s/geoserver/rest/layers/%s", GEOSERVER_HOST, GEOSERVER_PORT,
				deployment.getLayer());
		try {
			restTemplate.exchange(url, HttpMethod.DELETE, request, String.class);
		} catch (HttpClientErrorException exception) {
			// Check the status code. If it's a 404, then the layer has likely
			// already been deleted by some other means.
			if (exception.getStatusCode() == HttpStatus.NOT_FOUND) {
				logger.log(
						String.format(
								"Attempted to undeploy GeoServer layer %s while deleting the Deployment ID %s, but the layer was already deleted from GeoServer. This layer may have been removed by some other means.",
								deployment.getLayer(), deploymentId), PiazzaLogger.WARNING);
			} else {
				// Some other exception occurred. Bubble it up.
				throw exception;
			}
		}
		// Remove the Deployment from the Database
		accessor.deleteDeployment(deployment);
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
	private HttpStatus postGeoServerFeatureType(String restURL, String featureType) throws Exception {
		// Construct the URL for the Service
		String url = String.format(HOST_ADDRESS, GEOSERVER_HOST, GEOSERVER_PORT, restURL);
		System.out.println(String.format("Attempting to push a GeoServer Featuretype %s to URL %s", featureType, url));

		// Create the Request template and execute
		HttpHeaders headers = getGeoServerHeaders();
		headers.setContentType(MediaType.APPLICATION_XML);
		HttpEntity<String> request = new HttpEntity<String>(featureType, headers);

		ResponseEntity<String> response = null;
		try {
			response = restTemplate.exchange(url, HttpMethod.POST, request, String.class);
		} catch (Exception exception) {
			String error = String.format("There was an error creating the Coverage Layer to URL %s with errors %s",
					url, exception.getMessage());
			logger.log(error, PiazzaLogger.ERROR);
			throw new Exception(error);
		}

		// Return the HTTP Status
		return response.getStatusCode();
	}

	/**
	 * Gets the headers for a typical GeoServer request. This include the
	 * "application/XML" content, and the encoded basic credentials.
	 * 
	 * @return
	 */
	public HttpHeaders getGeoServerHeaders() {
		// Get the Basic authentication Headers for GeoServer
		String plainCredentials = String.format("%s:%s", GEOSERVER_USERNAME, GEOSERVER_PASSWORD);
		byte[] credentialBytes = plainCredentials.getBytes();
		byte[] encodedCredentials = Base64.encodeBase64(credentialBytes);
		String credentials = new String(encodedCredentials);
		HttpHeaders headers = new HttpHeaders();
		headers.add("Authorization", "Basic " + credentials);
		return headers;
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
