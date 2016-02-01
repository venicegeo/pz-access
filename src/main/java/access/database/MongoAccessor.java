package access.database;

import model.job.metadata.ResourceMetadata;

import org.springframework.stereotype.Component;

import access.database.model.Deployment;

/**
 * Handles Mongo access for the Deployer and the Leaser, and for the Resource
 * collection which stores the Ingested Resource metadata.
 * 
 * Deployments and leases have their own collections, and are managed by this
 * Access component.
 * 
 * @author Patrick.Doody
 * 
 */
@Component
public class MongoAccessor {
	/**
	 * Gets the Deployment for the specified Resource ID
	 * 
	 * @param resourceId
	 *            Resource ID
	 * @return The Deployment for the Resource, if any. Null, if none.
	 */
	public Deployment getDeploymentByResourceId(String resourceId) {
		throw new UnsupportedOperationException();
	}

	/**
	 * Creates a new Deployment entry in the database.
	 * 
	 * @param deployment
	 *            Deployment to enter
	 */
	public void insertDeployment(Deployment deployment) {
		throw new UnsupportedOperationException();
	}

	/**
	 * Gets the ResourceMetadata object by ID
	 * 
	 * @param resourceId
	 *            The ID of the ResourceMetadata
	 * @return ResourceMetadata object
	 */
	public ResourceMetadata getMetadataById(String resourceId) {
		throw new UnsupportedOperationException();
	}
}
