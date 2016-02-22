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

import model.data.DataResource;
import model.data.deployment.Deployment;
import model.data.type.PostGISResource;
import model.data.type.RasterResource;
import model.data.type.ShapefileResource;
import model.data.type.WfsResource;

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
	@Value("${geoserver.host}")
	private String GEOSERVER_HOST;
	@Value("${geoserver.port}")
	private String GEOSERVER_PORT;
	@Value("${geoserver.username}")
	private String GEOSERVER_USERNAME;
	@Value("${geoserver.password}")
	private String GEOSERVER_PASSWORD;
	private static final String ADD_LAYER_ENDPOINT = "http://%s:%s/geoserver/rest/workspaces/piazza/datastores/piazza/featuretypes/";
	private static final String CAPABILITIES_URL = "http://%s:%s/geoserver/piazza/wfs?service=wfs&version=2.0.0&request=GetCapabilities";

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
			if ((dataResource.getDataType() instanceof ShapefileResource)
					|| (dataResource.getDataType() instanceof PostGISResource)) {
				// Deploy from an existing PostGIS Table
				deployment = deployPostGisTable(dataResource);
			} else if (dataResource.getDataType() instanceof WfsResource) {
				// User has requested to deploy a WFS type resource. In this
				// case, there's nothing to deploy since the WFS is already
				// accessible by design? Just return the WFS information back to
				// them? Or return an error?
				deployment = null;
			} else if (dataResource.getDataType() instanceof RasterResource) {
				// Deploy a GeoTIFF to GeoServer
				throw new UnsupportedOperationException("GeoTIFF deployments not supported currently.");
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
		if (dataResource.getDataType() instanceof ShapefileResource) {
			tableName = ((ShapefileResource) dataResource.getDataType()).getDatabaseTableName();
		} else if (dataResource.getDataType() instanceof PostGISResource) {
			tableName = ((PostGISResource) dataResource.getDataType()).getTable();
		}

		// Inject the Metadata from the Data Resource into the Payload
		String requestBody = String.format(featureTypeRequestBody, tableName, tableName, tableName, dataResource
				.getSpatialMetadata().getEpsgString(), "EPSG:4326");

		// Execute the POST to GeoServer to add the FeatureType
		HttpStatus statusCode = postGeoServerFeatureType(requestBody);

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
		String capabilitiesUrl = String.format(CAPABILITIES_URL, GEOSERVER_HOST, GEOSERVER_PORT);
		Deployment deployment = new Deployment(deploymentId, dataResource.getDataId(), GEOSERVER_HOST, GEOSERVER_PORT,
				tableName, capabilitiesUrl);

		// Return the newly created Deployment
		return deployment;
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
	private HttpStatus postGeoServerFeatureType(String featureType) {
		// Get the Basic authentication Headers for GeoServer
		String plainCredentials = String.format("%s:%s", GEOSERVER_USERNAME, GEOSERVER_PASSWORD);
		byte[] credentialBytes = plainCredentials.getBytes();
		byte[] encodedCredentials = Base64.encodeBase64(credentialBytes);
		String credentials = new String(encodedCredentials);
		HttpHeaders headers = new HttpHeaders();
		headers.add("Authorization", "Basic " + credentials);
		headers.setContentType(MediaType.APPLICATION_XML);

		// Construct the endpoint for the Service
		String url = String.format(ADD_LAYER_ENDPOINT, GEOSERVER_HOST, GEOSERVER_PORT);

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
