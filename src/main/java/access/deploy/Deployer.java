package access.deploy;

import model.data.DataResource;

import org.springframework.stereotype.Component;

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

	/**
	 * Checks to see if the Resource ID currently has a deployment in the system
	 * or not.
	 * 
	 * @param resourceId
	 *            The resource ID to check.
	 * @return True if a deployment exists for the Resource matching the
	 *         Resource ID, false if not.
	 */
	public boolean doesDeploymentExist(String resourceId) {
		throw new UnsupportedOperationException();
	}

	/**
	 * Creates a new deployment from the dataResource object.
	 * 
	 * @param dataResource
	 *            The resource metadata, describing the object to be deployed.
	 * @return A deployment for the object.
	 */
	public Deployment createDeployment(DataResource dataResource) {
		// Get the Resources

		// Create the Deployment

		// Insert into Database

		// Return Deployment reference
		throw new UnsupportedOperationException();
	}
}
