package access.deploy;

import org.springframework.stereotype.Component;

/**
 * Handles the accessing of Resource files for deployment. When a Resource is to
 * be deployed, often times resources must be collected, either from disk or S3,
 * or some other data store. This class acts to facilitate the access of those
 * files so that they may be made accessible to components such as GeoServer.
 * 
 * @author Patrick.Doody
 * 
 */
@Component
public class ResourceAccessor {

}
