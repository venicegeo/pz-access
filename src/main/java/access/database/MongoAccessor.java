package access.database;

import org.springframework.stereotype.Component;

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

}
