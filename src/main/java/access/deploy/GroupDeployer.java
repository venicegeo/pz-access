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

import java.util.List;

import model.data.deployment.Deployment;
import model.data.deployment.DeploymentGroup;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import util.PiazzaLogger;
import util.UUIDFactory;
import access.database.Accessor;

/**
 * Component that handles the deployment of Group Layers on GeoServer. This is
 * done through the /deployment/group endpoint. Group layers refer to a
 * collection of layers and expose them all as a single WMS endpoint.
 * 
 * @author Patrick.Doody
 *
 */
@Component
public class GroupDeployer {
	@Autowired
	private PiazzaLogger logger;
	@Autowired
	private UUIDFactory uuidFactory;
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

	/**
	 * Creates a new Deployment Group, without specifying any initial Data
	 * Layers to be added. This will create the DeploymentGroup and store it in
	 * the database, however, the actual GeoServer Layer Group will not be
	 * created. The GeoServer Layer Group will be created upon first request of
	 * a layer to be added to that group.
	 * 
	 * @param createdBy
	 *            The user who requests this creation
	 * 
	 * @return Deployment Group, containing an ID that can be used for future
	 *         reference.
	 */
	public DeploymentGroup createDeploymentGroup(String createdBy) {
		// Commit the new group to the database and return immediately
		DeploymentGroup deploymentGroup = new DeploymentGroup(uuidFactory.getUUID(), createdBy);
		accessor.insertDeploymentGroup(deploymentGroup);
		return deploymentGroup;
	}

	/**
	 * Creates a new Deployment Group. For each Deployment specified in the
	 * constructor, it will add that deployment's layer to the Layer Group.
	 * 
	 * @param deployments
	 *            The list of Layers to add to the group.
	 * @param createdBy
	 *            The user who requests this creation
	 * @return Deployment Group, containing an ID that can be used for future
	 *         reference.
	 */
	public DeploymentGroup createDeploymentGroup(List<Deployment> deployments, String createdBy) {
		// Create the Group and commit it to the database
		DeploymentGroup deploymentGroup = createDeploymentGroup(createdBy);

		// For each Deployment, add this as a Layer to the Group.
		// TODO

		// Return the Group
		return deploymentGroup;
	}

	/**
	 * Adds Layers to the GeoServer Layer Group.
	 * 
	 * @param deploymentGroup
	 *            The layer group to concatenate Layers to.
	 * @param deployments
	 *            The deployments to add to the Layer Group.
	 */
	public void addDeploymentsToGroup(DeploymentGroup deploymentGroup, List<Deployment> deployments) {
		// TODO
		// Get the current Layers in the Group

		// Concatenate the new Layers to the Group

		// Update the Layer Group

	}

	/**
	 * Deletes a Deployment Group. This will remove the corresponding Layer
	 * 
	 * @param deploymentGroup
	 */
	public void deleteDeploymentGroup(DeploymentGroup deploymentGroup) {
		// Remove the Deployment Group from the Database
		// TODO

	}
}
