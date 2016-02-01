package access.deploy;

import org.springframework.stereotype.Component;

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

}
