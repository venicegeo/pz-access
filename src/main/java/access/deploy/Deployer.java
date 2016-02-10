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
import model.data.type.ShapefileResource;

import org.apache.commons.io.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import access.database.MongoAccessor;
import access.database.model.Deployment;

/**
 * Class that manages the Deployments held by this component. This is done by
 * managing the Deployments via a MongoDB collection.
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
	private MongoAccessor accessor;
	@Value("${geoserver.host}")
	private String GEOSERVER_HOST;

	/**
	 * Creates a new deployment from the dataResource object.
	 * 
	 * @param dataResource
	 *            The resource metadata, describing the object to be deployed.
	 * @return A deployment for the object.
	 */
	public Deployment createDeployment(DataResource dataResource) throws Exception {
		// Create the Deployment
		if (dataResource.getDataType() instanceof ShapefileResource) {
			// Deploy Shapefile
			try {
				String layerName = createShapefileLayer(dataResource);
			} catch (Exception exception) {
				exception.printStackTrace();
				throw new Exception("There was an error deploying to GeoServer: " + exception.getMessage());
			}
		} else {
			throw new UnsupportedOperationException("Cannot currently deploy this Data Type to GeoServer.");
		}

		// Update Deployment into the Access Database

		// Return Deployment reference
		return null;
	}

	/**
	 * Deploys a Shapefile resource to GeoServer. This will create a new
	 * GeoServer layer that will reference the PostGIS table that the shapefile
	 * has been transferred into.
	 * 
	 * TODO: Perhaps some failover if we can get the file, but we cannot get the
	 * tablespace? Such as trying to redeploy the tablespace.
	 * 
	 * @param dataResource
	 *            The DataResource for the Shapefile to deploy
	 * @return The layer name of the layer created on GeoServer
	 */
	private String createShapefileLayer(DataResource dataResource) throws IOException {
		// Create the JSON Payload for the Layer request to GeoServer
		ClassLoader classLoader = getClass().getClassLoader();
		String featureTypeRequestBody = IOUtils.toString(classLoader
				.getResourceAsStream("templates/featureTypeRequest.xml"));

		// Inject the Metadata from the Shapefile into the Payload
		ShapefileResource shapefileResource = (ShapefileResource) dataResource.getDataType();
		String requestBody = String.format(featureTypeRequestBody, shapefileResource.getDatabaseTableName(),
				shapefileResource.getDatabaseTableName(), shapefileResource.getDatabaseTableName(), dataResource
						.getSpatialMetadata().getCoordinateReferenceSystem(), "EPSG:4326");

		// Construct the GeoServer endpoint to point to.

		// Return the Layer Name
		return shapefileResource.getDatabaseTableName();
	}

	/**
	 * Returns an available GeoServer host for deployments. In a clustered
	 * scenario, there may be multiple GeoServer instances running. This method
	 * will fetch an available instance and return the hostname. This URL will
	 * be used to then POST the FeatureType to the server, thus deploying the
	 * new layer to that instance.
	 * 
	 * @return Hostname of an available GeoServer instance
	 */
	private String getAvailableGeoServerHost() {
		// TODO: If we have a cluster, add that logic here. For now, let's
		// assume we have a single instance.
		return GEOSERVER_HOST;
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
		Deployment deployment = accessor.getDeploymentByResourceId(dataId);
		if (deployment != null) {
			return true;
		} else {
			return false;
		}
	}
}
